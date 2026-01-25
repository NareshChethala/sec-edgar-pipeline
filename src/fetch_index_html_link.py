from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs


# -----------------------------
# Config
# -----------------------------
HEADERS = {"User-Agent": "Naresh Chethala Research (nchethala@wne.edu)"}
SEC_BASE = "https://www.sec.gov"


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


def write_parquet(df: pd.DataFrame, out_uri: str, compression: str):
    table = pa.Table.from_pandas(df, preserve_index=False)
    fs, path = fs_from_uri(out_uri)
    with fs.open_output_stream(path) as f:
        pq.write_table(table, f, compression=compression)


def make_part_uri(out_prefix: str, part: int, kind: str) -> str:
    out_prefix = out_prefix.rstrip("/")
    return f"{out_prefix}/{kind}-part-{part:06d}.parquet"


# -----------------------------
# Core parsing logic (UNCHANGED)
# -----------------------------
def parse_8k_html_link_from_index_html(index_url: str, index_html: str) -> Optional[str]:
    """
    Same selection logic as your working function:
      1) Prefer Complete submission text file (.txt)
      2) Else fallback to first 8-K typed document
    """
    soup = BeautifulSoup(index_html, "html.parser")
    table = soup.find("table", {"class": "tableFile"})
    if not table:
        return None

    best_8k_link = None

    for tr in table.find_all("tr"):
        cols = tr.find_all("td")
        if not cols:
            continue

        desc = cols[1].get_text(" ", strip=True).upper() if len(cols) > 1 else ""
        doc_a = cols[2].find("a") if len(cols) > 2 else None
        doc_href = doc_a.get("href") if doc_a else None
        doc_type = cols[3].get_text(" ", strip=True).upper() if len(cols) > 3 else ""

        if not doc_href:
            continue

        full_url = urljoin(SEC_BASE, doc_href)

        if "COMPLETE SUBMISSION TEXT FILE" in desc and doc_href.lower().endswith(".txt"):
            return full_url

        if doc_type.startswith("8-K") and best_8k_link is None:
            best_8k_link = full_url

    return best_8k_link


def fetch_index_html_and_link(index_url: str, sleep_seconds: float) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[str]]:
    """
    Returns (filing_link, index_html, status_code, error_message)
    """
    try:
        time.sleep(sleep_seconds)
        resp = requests.get(index_url, headers=HEADERS, timeout=15)
        status = resp.status_code
        html = resp.text if resp.text else None

        if status != 200:
            return None, html, status, f"HTTP {status}"

        if html is None:
            return None, None, status, "empty_html"

        filing_link = parse_8k_html_link_from_index_html(index_url, html)
        if not filing_link:
            return None, html, status, "no_8k_link_found"

        return filing_link, html, status, None

    except Exception as e:
        return None, None, None, str(e)


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Step 2: Fetch SEC index HTML and extract filing link; write success/errors parts.")
    ap.add_argument("--input", required=True, help="Input urlbuilt parquet file OR folder/prefix of urlbuilt parts (local/gs://s3://).")
    ap.add_argument("--url-col", default="sec_index_url", help="Column containing SEC index HTML URL (default: sec_index_url).")
    ap.add_argument("--out-prefix", required=True, help="Output folder/prefix for success/errors parts (local/gs://s3://).")
    ap.add_argument("--sleep-seconds", type=float, default=2.0, help="SEC-friendly delay per request (default: 2.0).")
    ap.add_argument("--compression", default="snappy", help="Parquet compression (default: snappy).")
    ap.add_argument("--max-files", type=int, default=0, help="Test mode: process only first N parquet files (0 = all).")
    ap.add_argument("--max-rowgroups", type=int, default=0, help="Test mode: process only first N rowgroups per file (0 = all).")
    args = ap.parse_args()

    sources = iter_input_sources(args.input)
    if args.max_files:
        sources = sources[: args.max_files]

    print(f"[INFO] HTML fetch input sources: {len(sources):,}")

    success_part = 0
    error_part = 0
    total_in = 0
    total_success = 0
    total_errors = 0

    for src in sources:
        for rg, nrg, df in iter_parquet_rowgroups(src, max_rowgroups=args.max_rowgroups):
            if args.url_col not in df.columns:
                raise ValueError(f"Missing required column '{args.url_col}' in {src}")

            rows_in = len(df)
            total_in += rows_in

            success_rows: List[Dict[str, Any]] = []
            error_rows: List[Dict[str, Any]] = []

            for _, row in df.iterrows():
                index_url = str(row[args.url_col]).strip()

                if not index_url or index_url.lower() == "nan":
                    error_rows.append({
                        "index_url": index_url,
                        "error": "missing_index_url",
                        "status_code": None,
                        "fetched_at": utc_now_iso(),
                        "index_html": None,
                    })
                    continue

                filing_link, index_html, status_code, err = fetch_index_html_and_link(
                    index_url, sleep_seconds=args.sleep_seconds
                )

                if filing_link:
                    success_rows.append({
                        "index_url": index_url,
                        "filing_link": filing_link,
                        "status_code": status_code,
                        "fetched_at": utc_now_iso(),
                        "index_html": index_html,
                    })
                else:
                    error_rows.append({
                        "index_url": index_url,
                        "error": err or "unknown_error",
                        "status_code": status_code,
                        "fetched_at": utc_now_iso(),
                        "index_html": index_html,
                    })

            success_df = pd.DataFrame(success_rows)
            errors_df = pd.DataFrame(error_rows)

            total_success += len(success_df)
            total_errors += len(errors_df)

            if not success_df.empty:
                out_uri = make_part_uri(args.out_prefix, success_part, "success")
                write_parquet(success_df, out_uri, compression=args.compression)
                print(f"[OK] wrote success {len(success_df):,} -> {out_uri}")
                success_part += 1

            if not errors_df.empty:
                out_uri = make_part_uri(args.out_prefix, error_part, "errors")
                write_parquet(errors_df, out_uri, compression=args.compression)
                print(f"[OK] wrote errors  {len(errors_df):,} -> {out_uri}")
                error_part += 1

            print(f"[INFO] {src} rowgroup {rg+1}/{nrg} | in={rows_in:,} success={len(success_df):,} errors={len(errors_df):,}")

    print("------------------------------------------------------------")
    print(f"[DONE] total input rows:     {total_in:,}")
    print(f"[DONE] total success rows:   {total_success:,}")
    print(f"[DONE] total error rows:     {total_errors:,}")
    print(f"[DONE] success parts written:{success_part:,}")
    print(f"[DONE] error parts written:  {error_part:,}")


if __name__ == "__main__":
    main()