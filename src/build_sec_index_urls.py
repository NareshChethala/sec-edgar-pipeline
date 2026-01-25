from __future__ import annotations

import argparse
import re
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs


# -----------------------------
# Core logic (UNCHANGED)
# -----------------------------
def build_sec_url(filename: str) -> Optional[str]:
    """
    Converts the Filename path from master.idx into a valid SEC filing URL.
    Example input: 'edgar/data/320187/0000320187-25-000060.txt'
    Output: 'https://www.sec.gov/Archives/edgar/data/320187/000032018725000060/0000320187-25-000060-index.html'
    """
    try:
        match = re.search(r"edgar/data/(\d+)/(\d{10}-\d{2}-\d{6})", str(filename))
        if not match:
            return None
        cik = match.group(1)
        accession = match.group(2)
        accession_no_dash = accession.replace("-", "")
        url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_dash}/{accession}-index.html"
        return url
    except Exception:
        return None


# -----------------------------
# Helpers
# -----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fs_from_uri(uri: str) -> Tuple[pafs.FileSystem, str]:
    fs, path = pafs.FileSystem.from_uri(uri)
    return fs, path


def is_parquet_path(p: str) -> bool:
    return p.lower().endswith(".parquet")


def list_parquets(prefix_uri: str) -> List[str]:
    """
    List .parquet files under a folder/prefix for local/gs://s3://
    Returns full URIs.
    """
    fs, base_path = fs_from_uri(prefix_uri)
    base_path = base_path.rstrip("/")
    is_local = not (prefix_uri.startswith("gs://") or prefix_uri.startswith("s3://"))

    selector = pafs.FileSelector(base_path + "/", recursive=True)
    infos = fs.get_file_info(selector)

    out: List[str] = []
    for info in infos:
        if info.type == pafs.FileType.File and info.path.lower().endswith(".parquet"):
            if is_local:
                out.append(info.path)
            else:
                scheme = prefix_uri.split("://", 1)[0]
                out.append(f"{scheme}://{info.path}")
    return sorted(out)


def iter_input_sources(input_uri: str) -> List[str]:
    return [input_uri] if is_parquet_path(input_uri) else list_parquets(input_uri)


def iter_parquet_rowgroups(uri: str, max_rowgroups: int = 0):
    fs, path = fs_from_uri(uri)
    with fs.open_input_file(path) as f:
        pf = pq.ParquetFile(f)
        nrg = pf.num_row_groups
        for rg in range(nrg):
            if max_rowgroups and rg >= max_rowgroups:
                break
            table = pf.read_row_group(rg)
            yield rg, nrg, table.to_pandas(types_mapper=pd.ArrowDtype)


def make_part_uri(out_prefix: str, part: int) -> str:
    out_prefix = out_prefix.rstrip("/")
    return f"{out_prefix}/urlbuilt-part-{part:06d}.parquet"


def write_parquet(df: pd.DataFrame, out_uri: str, compression: str):
    table = pa.Table.from_pandas(df, preserve_index=False)
    fs, path = fs_from_uri(out_uri)
    with fs.open_output_stream(path) as f:
        pq.write_table(table, f, compression=compression)


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Step 1: Build SEC index URLs and write urlbuilt parquet parts.")
    ap.add_argument("--input", required=True, help="Input parquet file OR folder/prefix of parquet parts (local/gs://s3://).")
    ap.add_argument("--filename-col", default="Filename", help="Column containing the master.idx filename path (default: Filename).")
    ap.add_argument("--out-prefix", required=True, help="Output folder/prefix to write urlbuilt-part-*.parquet (local/gs://s3://).")
    ap.add_argument("--compression", default="snappy", help="Parquet compression (default: snappy).")
    ap.add_argument("--max-files", type=int, default=0, help="Test mode: process only first N parquet files (0 = all).")
    ap.add_argument("--max-rowgroups", type=int, default=0, help="Test mode: process only first N rowgroups per file (0 = all).")
    args = ap.parse_args()

    sources = iter_input_sources(args.input)
    if args.max_files:
        sources = sources[: args.max_files]

    print(f"[INFO] URL build input sources: {len(sources):,}")
    part = 0
    total_in = 0
    total_out = 0

    for src in sources:
        for rg, nrg, df in iter_parquet_rowgroups(src, max_rowgroups=args.max_rowgroups):
            if args.filename_col not in df.columns:
                raise ValueError(f"Missing required column '{args.filename_col}' in {src}")

            in_rows = len(df)
            total_in += in_rows

            # Add URL column (core logic), drop invalid
            df["sec_index_url"] = df[args.filename_col].apply(build_sec_url)
            df = df.dropna(subset=["sec_index_url"]).copy()
            df["urlbuilt_at"] = utc_now_iso()

            out_rows = len(df)
            total_out += out_rows

            if out_rows:
                out_uri = make_part_uri(args.out_prefix, part)
                write_parquet(df, out_uri, compression=args.compression)
                print(f"[OK] wrote {out_rows:,} -> {out_uri}")
                part += 1

            print(f"[INFO] {src} rowgroup {rg+1}/{nrg} | in={in_rows:,} out={out_rows:,}")

    print("------------------------------------------------------------")
    print(f"[DONE] total input rows:  {total_in:,}")
    print(f"[DONE] total output rows: {total_out:,}")
    print(f"[DONE] parts written:     {part:,}")


if __name__ == "__main__":
    main()