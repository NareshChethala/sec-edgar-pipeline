from __future__ import annotations

import time
from typing import Tuple, List, Dict, Any, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import requests
from bs4 import BeautifulSoup

from src.common.imports import argparse


# ============================================================
# CONSTANTS
# ============================================================
TENK_FORMS = {"10-K", "10-K/A"}


# ============================================================
# STORAGE HELPERS (GCS / local)
# ============================================================
def is_gcs_path(p: str) -> bool:
    return p.strip().lower().startswith("gs://")


def parse_gcs_url(gs_url: str) -> Tuple[str, str]:
    s = gs_url.strip()
    if not s.startswith("gs://"):
        raise ValueError(f"Not a GCS URL: {gs_url}")
    rest = s[5:]
    parts = rest.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) == 2 else ""
    return bucket, key


def gcs_exists(gs_url: str) -> bool:
    bucket, key = parse_gcs_url(gs_url)
    fs = pafs.GcsFileSystem()
    info = fs.get_file_info(f"{bucket}/{key}")
    return info.type == pafs.FileType.File


# ============================================================
# URL FIX (ixviewer) - included for robustness (no behavior change)
# ============================================================
def fix_ixviewer_url(url: Optional[str]) -> Optional[str]:
    if isinstance(url, str) and "ix?doc=" in url:
        return url.replace("https://www.sec.gov/ix?doc=", "https://www.sec.gov")
    return url


# ============================================================
# 1) YOUR WORKING HTML LOADER (core preserved)
# ============================================================
def extract_filing_html_directly(row, user_agent_email: str):
    try:
        filename = str(row.get("Filename", "")).strip().replace(" ", "")
        if not filename:
            return None, None, "❌ Missing or invalid Filename"

        headers = {"User-Agent": user_agent_email}

        # ------------------------------------------------------------
        # CASE 1: Filename already points directly to a document
        # Example: edgar/data/861439/000091205794000263.txt
        # ------------------------------------------------------------
        if filename.lower().endswith((".txt", ".htm", ".html")):
            filing_url = f"https://www.sec.gov/Archives/{filename.lstrip('/')}"
            resp = requests.get(filing_url, headers=headers, timeout=25)
            if resp.status_code != 200:
                return filing_url, None, f"❌ Filing fetch failed: {resp.status_code}"
            return filing_url, resp.text, "✅ Success (direct)"

        # ------------------------------------------------------------
        # CASE 2: Modern format where Filename includes accession folder
        # Example: edgar/data/320193/000032019318000145/a10-k20189292018.htm
        # We derive the index URL using the accession folder.
        # ------------------------------------------------------------
        parts = filename.split("/")
        if len(parts) < 4:
            return None, None, f"⚠️ Invalid path: {filename}"

        cik = parts[2]
        accession_nodash = parts[3]  # folder (typically no dashes)
        accession_dashes = None

        # Build index URL using the accession folder name
        index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodash}/index.html"

        resp = requests.get(index_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return index_url, None, f"❌ Index fetch failed: {resp.status_code}"

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", class_="tableFile")
        if not table:
            return index_url, None, "⚠️ No document table found"

        # accept .htm and .html, ignore index files
        link_tag = table.find(
            "a",
            href=lambda h: h and (h.endswith(".htm") or h.endswith(".html")) and "index" not in h.lower()
        )
        if not link_tag:
            return index_url, None, "⚠️ No primary .htm/.html link found"

        filing_path = link_tag["href"].lstrip("/")
        filing_url = f"https://www.sec.gov/{filing_path}"

        f_resp = requests.get(filing_url, headers=headers, timeout=25)
        if f_resp.status_code != 200:
            return filing_url, None, f"❌ Filing fetch failed: {f_resp.status_code}"

        return filing_url, f_resp.text, "✅ Success (index)"

    except Exception as e:
        return None, None, f"⚠️ Exception: {e}"

# ============================================================
# 2) YOUR CLEANING FUNCTION (core preserved)
# ============================================================
def clean_filing_html(html_text):
    """Converts raw SEC filing HTML into readable plain text."""
    try:
        soup = BeautifulSoup(html_text, "html.parser")
        for tag in soup(["script", "style", "header", "footer", "nav", "noscript", "meta"]):
            tag.decompose()
        body = soup.find("body")
        raw_text = body.get_text(separator="\n") if body else soup.get_text(separator="\n")
        lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
        return "\n".join(lines)
    except Exception as e:
        return f"⚠️ Cleaning failed: {e}"


# ============================================================
# 3) PIPELINE: STREAM INPUT IN BATCHES + WRITE OUTPUT IN PARTS
# ============================================================
def write_part_gcs(records: List[Dict[str, Any]], out_url: str) -> None:
    out_bucket, out_key = parse_gcs_url(out_url)
    fs = pafs.GcsFileSystem()
    table = pa.Table.from_pandas(pd.DataFrame(records), preserve_index=False)
    with fs.open_output_stream(f"{out_bucket}/{out_key}") as out_stream:
        pq.write_table(table, out_stream, compression="snappy")


def write_part_local(records: List[Dict[str, Any]], out_path: str) -> None:
    from pathlib import Path
    p = Path(out_path).expanduser().resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(records).to_parquet(p, index=False, engine="pyarrow")


def main():
    parser = argparse.ArgumentParser(
        description="Stream a 10-K metadata Parquet (GCS/local), scrape filing HTML in a RAM-safe way, and write Parquet parts (GCS/local)."
    )
    parser.add_argument("--input", required=True, help="Input parquet (local path or gs://...) containing at least Filename and Form Type.")
    parser.add_argument("--output-prefix", required=True, help="Output folder (local folder or gs://bucket/prefix) for parquet parts.")
    parser.add_argument("--user-agent", required=True, help='SEC compliant user agent, e.g. "Name email@domain.com"')

    parser.add_argument("--delay", type=float, default=1.5, help="Seconds to sleep between filings (default 1.5).")
    parser.add_argument("--retry-limit", type=int, default=2, help="Retries per filing (default 2).")
    parser.add_argument("--checkpoint-every", type=int, default=200, help="Flush output part every N successful filings (default 200).")
    parser.add_argument("--max-rowgroups", type=int, default=0, help="Test: process only first N rowgroups (0=all).")
    parser.add_argument("--max-filings", type=int, default=0, help="Test: stop after N filings total (0=all).")
    parser.add_argument("--skip-if-exists", action="store_true", help="Skip writing a part if it already exists.")

    args = parser.parse_args()

    # Open parquet streaming
    if is_gcs_path(args.input):
        b, k = parse_gcs_url(args.input)
        fs = pafs.GcsFileSystem()
        f = fs.open_input_file(f"{b}/{k}")
        pf = pq.ParquetFile(f)
        input_mode = "gcs"
    else:
        pf = pq.ParquetFile(args.input)
        input_mode = "local"

    out_part = 0
    ok_in_part = 0
    total_seen = 0
    total_ok = 0
    buffer: List[Dict[str, Any]] = []

    def part_url(part_idx: int) -> str:
        if is_gcs_path(args.output_prefix):
            out_bucket, out_prefix = parse_gcs_url(args.output_prefix)
            out_prefix = out_prefix.strip("/")
            return f"gs://{out_bucket}/{out_prefix}/part-{part_idx:06d}.parquet"
        else:
            from pathlib import Path
            out_dir = Path(args.output_prefix).expanduser().resolve()
            return str(out_dir / f"part-{part_idx:06d}.parquet")

    def flush_part():
        nonlocal out_part, ok_in_part, buffer
        if not buffer:
            return

        out = part_url(out_part)

        if args.skip_if_exists and is_gcs_path(out) and gcs_exists(out):
            print(f"[SKIP] exists: {out}")
            buffer = []
            out_part += 1
            ok_in_part = 0
            return

        if args.skip_if_exists and (not is_gcs_path(out)):
            from pathlib import Path
            if Path(out).expanduser().exists():
                print(f"[SKIP] exists: {out}")
                buffer = []
                out_part += 1
                ok_in_part = 0
                return

        if is_gcs_path(out):
            write_part_gcs(buffer, out)
        else:
            write_part_local(buffer, out)

        print(f"[OK] wrote {len(buffer):,} rows -> {out}")
        buffer = []
        out_part += 1
        ok_in_part = 0

    print("------------------------------------------------------------")
    print(f"[INFO] input={args.input} ({input_mode})")
    print(f"[INFO] output-prefix={args.output_prefix}")
    print(f"[INFO] delay={args.delay}s retry_limit={args.retry_limit} checkpoint_every={args.checkpoint_every}")
    print("------------------------------------------------------------")

    for rg in range(pf.num_row_groups):
        if args.max_rowgroups and rg >= args.max_rowgroups:
            break

        df = pf.read_row_group(rg).to_pandas()

        # normalize columns for filtering
        if "Form Type" in df.columns:
            df["Form Type"] = df["Form Type"].astype("string").str.strip().str.upper()
            df = df[df["Form Type"].isin(TENK_FORMS)].copy()

        if df.empty:
            print(f"[ROWGROUP {rg+1:03d}/{pf.num_row_groups}] empty after filter")
            continue

        df = df.reset_index(drop=True)

        print(f"[ROWGROUP {rg+1:03d}/{pf.num_row_groups}] candidates={len(df):,}")

        for i in range(len(df)):
            row = df.iloc[i]
            total_seen += 1

            if args.max_filings and total_seen > args.max_filings:
                flush_part()
                print("[DONE] max_filings reached.")
                print(f"[DONE] seen={total_seen:,} ok={total_ok:,}")
                return

            # ---- your retry loop (core behavior) ----
            url = None
            html_text = None
            status = None

            for attempt in range(args.retry_limit):
                url, html_text, status = extract_filing_html_directly(row, args.user_agent)
                if html_text:
                    break
                else:
                    # brief backoff (same idea as your code)
                    time.sleep(2)

            if not html_text:
                # store failures too? (optional)
                # To match your notebook, we store a failure record WITHOUT html/text
                rec_fail: Dict[str, Any] = {
                    "status": status,
                    "filing_url": url,
                    "filing_text": None,
                    "cleaned_text": None,
                }
                for col in df.columns:
                    rec_fail[col.replace(" ", "_").lower()] = row[col]
                buffer.append(rec_fail)

                # even failures count toward flushing so parts are not huge
                # but your notebook checkpoints by N filings processed; we flush by N OK
                # (keeping it efficient). If you want flush by total records, say so.
                continue

            # success
            cleaned = clean_filing_html(html_text)
            total_ok += 1
            ok_in_part += 1

            rec: Dict[str, Any] = {
                "status": "✅ Success",
                "filing_url": url,
                "filing_text": html_text,    # raw html
                "cleaned_text": cleaned,     # plain text
            }

            # keep all metadata columns present in input
            for col in df.columns:
                rec[col.replace(" ", "_").lower()] = row[col]

            buffer.append(rec)

            # checkpoint flush based on OK filings
            if ok_in_part >= args.checkpoint_every:
                flush_part()

            # SEC rate limit
            time.sleep(args.delay)

        # rowgroup done: flush whatever is in buffer (optional)
        # To keep files aligned with your "checkpoint every N filings" behavior,
        # we do NOT auto-flush here unless you want it. It’s fine either way.
        print(f"[INFO] rg done | seen={total_seen:,} ok={total_ok:,} buffered={len(buffer):,}")

    # final flush
    flush_part()
    print("------------------------------------------------------------")
    print(f"[DONE] seen={total_seen:,}")
    print(f"[DONE] ok={total_ok:,}")
    print(f"[DONE] parts_written={out_part:,}")


if __name__ == "__main__":
    main()