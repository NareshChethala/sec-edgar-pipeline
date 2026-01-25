from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
from tqdm import tqdm


# -----------------------------
# Helpers
# -----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def normalize_quarter(q) -> str:
    """
    Accepts: 1,2,3,4 or '1','2' or 'Q1','q1','Quarter 1', etc.
    Returns: 'q1'...'q4' or 'q?' if unknown (we will skip unknown).
    """
    if q is None or (isinstance(q, float) and pd.isna(q)):
        return ""
    s = str(q).strip().lower()
    s = s.replace("quarter", "").replace(" ", "")
    if s in {"1", "q1"}: return "q1"
    if s in {"2", "q2"}: return "q2"
    if s in {"3", "q3"}: return "q3"
    if s in {"4", "q4"}: return "q4"
    return ""

def ensure_str_cik(cik) -> str:
    # SEC CIKs are typically zero-padded to 10 digits for consistency
    s = str(cik).strip()
    if s.isdigit():
        return s.zfill(10)
    return s

def build_primary_key(df: pd.DataFrame) -> pd.Series:
    # pk = cik|form_type|date_filed|filename
    cik = df["cik"].map(ensure_str_cik)
    form_type = df["form_type"].astype(str).fillna("")
    date_filed = df["date_filed"].astype(str).fillna("")
    filename = df["filename"].astype(str).fillna("")
    return cik + "|" + form_type + "|" + date_filed + "|" + filename

def split_gcs_uri(gs_uri: str) -> Tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// URI, got: {gs_uri}")
    rest = gs_uri[5:]
    bucket, _, path = rest.partition("/")
    return bucket, path

def list_gcs_parquets(fs: pafs.FileSystem, bucket: str, prefix: str) -> List[str]:
    """
    Returns full gs:// URIs for all .parquet objects under prefix.
    """
    selector = pafs.FileSelector(f"{bucket}/{prefix}", recursive=True)
    infos = fs.get_file_info(selector)
    out = []
    for info in infos:
        if info.type == pafs.FileType.File and info.path.endswith(".parquet"):
            out.append("gs://" + info.path)
    return sorted(out)

def read_manifest(fs: pafs.FileSystem, manifest_uri: str) -> set:
    """
    Manifest stores processed input file URIs (one per line JSONL).
    """
    bucket, path = split_gcs_uri(manifest_uri)
    try:
        with fs.open_input_file(f"{bucket}/{path}") as f:
            processed = set()
            for line in f.read().decode("utf-8").splitlines():
                if not line.strip():
                    continue
                obj = json.loads(line)
                processed.add(obj["input_file"])
            return processed
    except Exception:
        return set()

def append_manifest(fs: pafs.FileSystem, manifest_uri: str, records: List[dict]) -> None:
    bucket, path = split_gcs_uri(manifest_uri)
    # open_output_stream overwrites; so we "append" by reading old + writing new (manifest should stay small)
    existing_lines = []
    try:
        with fs.open_input_file(f"{bucket}/{path}") as f:
            existing_lines = f.read().decode("utf-8").splitlines()
    except Exception:
        existing_lines = []

    new_lines = existing_lines + [json.dumps(r, ensure_ascii=False) for r in records]
    tmp_path = f"{bucket}/{path}.tmp"

    with fs.open_output_stream(tmp_path) as out:
        out.write(("\n".join(new_lines) + "\n").encode("utf-8"))

    # atomic-ish replace
    fs.delete_file(f"{bucket}/{path}") if fs.get_file_info(f"{bucket}/{path}").type == pafs.FileType.File else None
    fs.move(tmp_path, f"{bucket}/{path}")

def safe_read_parquet_to_df(fs: pafs.FileSystem, uri: str, columns: List[str] | None = None) -> pd.DataFrame:
    bucket, path = split_gcs_uri(uri)
    with fs.open_input_file(f"{bucket}/{path}") as f:
        table = pq.read_table(f, columns=columns)
    return table.to_pandas()

def safe_write_df_to_parquet(fs: pafs.FileSystem, df: pd.DataFrame, out_uri: str) -> None:
    bucket, path = split_gcs_uri(out_uri)
    table = pa.Table.from_pandas(df, preserve_index=False)

    tmp_uri = out_uri + ".tmp"
    tmp_bucket, tmp_path = split_gcs_uri(tmp_uri)

    with fs.open_output_stream(f"{tmp_bucket}/{tmp_path}") as f:
        pq.write_table(table, f, compression="zstd")

    # replace target
    info = fs.get_file_info(f"{bucket}/{path}")
    if info.type == pafs.FileType.File:
        fs.delete_file(f"{bucket}/{path}")
    fs.move(f"{tmp_bucket}/{tmp_path}", f"{bucket}/{path}")

def acquire_local_lock(lock_path: str) -> None:
    """
    Prevent two local processes from running simultaneously on the VM.
    """
    if os.path.exists(lock_path):
        raise RuntimeError(f"Lock exists at {lock_path}. Another job may be running.")
    with open(lock_path, "w") as f:
        f.write(utc_now_iso())

def release_local_lock(lock_path: str) -> None:
    try:
        os.remove(lock_path)
    except FileNotFoundError:
        pass


# -----------------------------
# Core job
# -----------------------------
def process_batch(
    fs: pafs.FileSystem,
    input_files: List[str],
    out_base: str,
    *,
    required_cols: List[str],
) -> Dict[Tuple[int, str], pd.DataFrame]:
    """
    Reads a batch of input parquet files, adds primary_key, normalizes quarter,
    and groups rows by (year, quarter).
    Returns: dict[(year, 'q1'..'q4')] = dataframe for that group.
    """
    grouped: Dict[Tuple[int, str], List[pd.DataFrame]] = {}

    for uri in input_files:
        df = safe_read_parquet_to_df(fs, uri, columns=None)  # read all columns to preserve your current schema
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            print(f"[WARN] Skipping {uri} missing columns: {missing}")
            continue

        # normalize + PK
        df["quarter_norm"] = df["quarter"].map(normalize_quarter)
        df = df[df["quarter_norm"].isin(["q1", "q2", "q3", "q4"])].copy()
        if df.empty:
            continue

        # ensure year int
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        df = df[df["year"].notna()].copy()
        if df.empty:
            continue

        df["primary_key"] = build_primary_key(df)
        df["indexed_at"] = utc_now_iso()  # useful for audits; harmless

        # group
        for (y, q), sub in df.groupby(["year", "quarter_norm"], dropna=True):
            y_int = int(y)
            grouped.setdefault((y_int, q), []).append(sub.drop(columns=["quarter_norm"]))

    # concat per group
    out: Dict[Tuple[int, str], pd.DataFrame] = {}
    for k, parts in grouped.items():
        out[k] = pd.concat(parts, ignore_index=True)

    return out

def quarter_output_uri(out_base: str, year: int, quarter: str) -> str:
    # EXACT format you requested:
    # gs://.../quarter/year=YYYY/q1.parquet
    return f"{out_base}/year={year}/{quarter}.parquet"

def merge_into_quarter_files(
    fs: pafs.FileSystem,
    out_base: str,
    grouped_rows: Dict[Tuple[int, str], pd.DataFrame],
) -> None:
    """
    For each (year, quarter), read existing q*.parquet (if present),
    append new rows, and overwrite the quarter file.
    """
    for (year, quarter), new_df in grouped_rows.items():
        out_uri = quarter_output_uri(out_base, year, quarter)
        bucket, path = split_gcs_uri(out_uri)
        info = fs.get_file_info(f"{bucket}/{path}")

        if info.type == pafs.FileType.File:
            # read old + append new
            old_df = safe_read_parquet_to_df(fs, out_uri, columns=None)
            merged = pd.concat([old_df, new_df], ignore_index=True)
        else:
            merged = new_df

        safe_write_df_to_parquet(fs, merged, out_uri)
        print(f"[OK] Wrote {len(new_df):,} new rows into {out_uri} (total now {len(merged):,})")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-prefix", required=True,
                    help="GCS prefix containing incoming parquet parts (no wildcard), e.g. gs://bucket/path/to/parts")
    ap.add_argument("--out-base", required=True,
                    help="GCS base output folder, e.g. gs://bucket/edgar_10k_html/quarter")
    ap.add_argument("--manifest", required=True,
                    help="GCS URI for processed manifest jsonl, e.g. gs://bucket/manifests/quarter_index_manifest.jsonl")
    ap.add_argument("--batch-files", type=int, default=250,
                    help="How many input parquet files to process per batch")
    ap.add_argument("--sleep-seconds", type=int, default=0,
                    help="If >0, runs forever: after each pass, sleep this many seconds and check for new files.")
    ap.add_argument("--lock-path", default="/tmp/quarter_index.lock",
                    help="Local lock path to prevent concurrent runs on the same VM")
    args = ap.parse_args()

    acquire_local_lock(args.lock_path)

    try:
        fs = pafs.GcsFileSystem()

        in_bucket, in_prefix = split_gcs_uri(args.input_prefix)
        if in_prefix.endswith("/"):
            in_prefix = in_prefix[:-1]

        required_cols = ["year", "quarter", "cik", "form_type", "date_filed", "filename"]

        while True:
            processed = read_manifest(fs, args.manifest)

            # list all input parquets
            all_files = list_gcs_parquets(fs, in_bucket, in_prefix)
            new_files = [f for f in all_files if f not in processed]

            print(f"[INFO] Total files: {len(all_files):,} | New files: {len(new_files):,}")

            if not new_files:
                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)
                    continue
                else:
                    break

            # process in batches of files
            manifest_records = []
            for i in range(0, len(new_files), args.batch_files):
                batch = new_files[i:i + args.batch_files]
                print(f"[INFO] Processing batch {i//args.batch_files + 1} with {len(batch)} files...")

                grouped = process_batch(fs, batch, args.out_base, required_cols=required_cols)
                merge_into_quarter_files(fs, args.out_base, grouped)

                # update manifest
                for uri in batch:
                    manifest_records.append({
                        "input_file": uri,
                        "processed_at": utc_now_iso(),
                    })

                # write manifest after each batch (safer)
                append_manifest(fs, args.manifest, manifest_records)
                manifest_records = []

            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)
            else:
                break

        print("[DONE] Quarter indexing complete.")

    finally:
        release_local_lock(args.lock_path)


if __name__ == "__main__":
    main()