from __future__ import annotations

import gzip
import re
import time
from io import BytesIO
from pathlib import Path
from typing import Optional, Tuple, List, Set

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.common.imports import argparse, require_gcs


VALID_TYPES = {"10-K", "10-K/A"}
SEC_BASE = "https://www.sec.gov"
ARCHIVES_BASE = "https://www.sec.gov/Archives"


# ---------------------------
# Small utilities
# ---------------------------

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
    return bucket, key.strip("/")


def normalize_cik(x: str) -> str:
    s = str(x).strip()
    s = re.sub(r"\.0$", "", s)
    s = re.sub(r"\D", "", s)  # digits only
    return s


def parse_csv_list(s: Optional[str]) -> Optional[List[str]]:
    if not s:
        return None
    return [x.strip() for x in s.split(",") if x.strip()]


def gzip_text(text: str) -> bytes:
    raw = text.encode("utf-8", errors="ignore")
    out = BytesIO()
    with gzip.GzipFile(fileobj=out, mode="wb") as gz:
        gz.write(raw)
    return out.getvalue()


def request_with_retries(
    session: requests.Session,
    url: str,
    headers: dict,
    timeout: int,
    max_retries: int,
) -> Optional[requests.Response]:
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r

            # Common SEC outcomes: 403/429 if rate/headers/auth issues
            wait = min(2 * attempt, 10)
            print(f"[WARN] {r.status_code} for {url} (attempt {attempt}/{max_retries}), sleeping {wait}s")
            time.sleep(wait)
        except Exception as e:
            wait = min(2 * attempt, 10)
            print(f"[WARN] Exception for {url} (attempt {attempt}/{max_retries}): {type(e).__name__}: {e}. Sleeping {wait}s")
            time.sleep(wait)

    return None


# ---------------------------
# Input loading (local/GCS)
# ---------------------------

def load_idx_df(input_path: str) -> pd.DataFrame:
    """
    Loads your parsed idx dataset (from parse_idx), must include at least:
    - Filename
    - Form Type
    - CIK
    - Year   (Year is from idx source filename in your pipeline)
    """
    if is_gcs_path(input_path):
        require_gcs()
        from google.cloud import storage

        bucket, key = parse_gcs_url(input_path)
        client = storage.Client()
        blob = client.bucket(bucket).blob(key)
        data = blob.download_as_bytes()
        bio = BytesIO(data)

        if input_path.lower().endswith(".csv"):
            return pd.read_csv(bio, dtype=str)
        if input_path.lower().endswith(".parquet"):
            return pd.read_parquet(bio, engine="pyarrow")
        raise ValueError("--input on GCS must end with .csv or .parquet")

    p = Path(input_path).expanduser().resolve()
    if not p.exists():
        raise ValueError(f"Input file not found: {p}")
    if p.suffix.lower() == ".csv":
        return pd.read_csv(p, dtype=str)
    if p.suffix.lower() == ".parquet":
        return pd.read_parquet(p, engine="pyarrow")
    raise ValueError("--input must be .csv or .parquet")


# ---------------------------
# SEC URL building (from idx row)
# ---------------------------

def build_index_url_from_filename(filename: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    IDX 'Filename' example:
      edgar/data/320193/0000320193-20-000096.txt

    Returns:
      (index_url, cik, accession_with_dashes)
    """
    fn = str(filename).strip().replace(" ", "")
    parts = fn.split("/")
    if len(parts) < 4:
        return None, None, None

    cik = parts[2].strip()
    accession = parts[3].strip().replace(".txt", "")  # keep dashes
    accession_nodashes = accession.replace("-", "")
    index_filename = f"{accession}-index.htm"

    index_url = f"{ARCHIVES_BASE}/edgar/data/{cik}/{accession_nodashes}/{index_filename}"
    return index_url, cik, accession


def choose_primary_doc_href(index_html: str) -> Optional[str]:
    """
    Choose primary .htm filing doc from SEC index page.
    Prefer document rows whose Type is 10-K or 10-K/A, else fallback to first .htm (not -index.htm).
    """
    soup = BeautifulSoup(index_html, "html.parser")
    table = soup.find("table", class_="tableFile")
    if table is None:
        return None

    rows = table.find_all("tr")
    # Prefer matching doc type in the Type column
    for tr in rows[1:]:
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        doc_type = (tds[3].get_text(strip=True) or "").upper()
        a = tr.find("a", href=True)
        if not a:
            continue
        href = a["href"]
        if not href.lower().endswith(".htm"):
            continue
        if href.lower().endswith("-index.htm"):
            continue
        if doc_type in VALID_TYPES:
            return href

    # Fallback: first .htm not index
    a = table.find("a", href=lambda h: h and h.lower().endswith(".htm") and not h.lower().endswith("-index.htm"))
    return a["href"] if a else None


def normalize_href_to_url(href: str) -> str:
    href = href.strip()
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"{SEC_BASE}{href}"
    return f"{SEC_BASE}/{href}"


def fetch_10k_html_for_row(
    session: requests.Session,
    row: pd.Series,
    user_agent: str,
    max_retries: int,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Returns (filing_url, html_text, cik, accession_with_dashes)
    """
    index_url, cik, accession = build_index_url_from_filename(row["Filename"])
    if not index_url:
        return None, None, None, None

    headers = {"User-Agent": user_agent}

    r_index = request_with_retries(session, index_url, headers=headers, timeout=20, max_retries=max_retries)
    if not r_index:
        print(f"[FAIL] Index page: {index_url}")
        return None, None, cik, accession

    href = choose_primary_doc_href(r_index.text)
    if not href:
        print(f"[FAIL] No filing doc link found: {index_url}")
        return None, None, cik, accession

    filing_url = normalize_href_to_url(href)
    r_filing = request_with_retries(session, filing_url, headers=headers, timeout=40, max_retries=max_retries)
    if not r_filing:
        print(f"[FAIL] Filing page: {filing_url}")
        return filing_url, None, cik, accession

    return filing_url, r_filing.text, cik, accession


# ---------------------------
# Output writers (local/GCS)
# ---------------------------

def gcs_exists(bucket: str, key: str) -> bool:
    require_gcs()
    from google.cloud import storage
    client = storage.Client()
    blob = client.bucket(bucket).blob(key)
    return blob.exists(client)


def gcs_upload_bytes(bucket: str, key: str, data: bytes) -> None:
    require_gcs()
    from google.cloud import storage
    client = storage.Client()
    blob = client.bucket(bucket).blob(key)
    blob.upload_from_string(data, content_type="application/gzip")


def build_output_relpath(cik: str, year: str, accession: str) -> str:
    # accession may include slashes? it should not. keep it safe:
    accession = accession.replace("/", "_")
    return f"cik={cik}/year={year}/accession={accession}.html.gz"


# ---------------------------
# Main CLI
# ---------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Download primary 10-K / 10-K/A HTML from SEC using parsed company.idx dataset."
    )

    # Input
    parser.add_argument("--input", required=True, help="Parsed idx dataset (.parquet or .csv). Local path or gs://bucket/object.")
    parser.add_argument("--user-agent", required=True, help='SEC required User-Agent, e.g. "Naresh Chethala naresh@example.com".')

    # Selection
    parser.add_argument("--cik", help="Single CIK to select.")
    parser.add_argument("--ciks", help="Comma-separated CIK list, e.g. '320193,789019'.")
    parser.add_argument("--year", type=int, help="Single year to select (based on idx source year).")
    parser.add_argument("--years", help="Comma-separated year list, e.g. '2019,2020,2021'.")
    parser.add_argument("--all", action="store_true", help="Download ALL 10-K/10-K/A rows (ignores cik/year filters).")
    parser.add_argument("--limit", type=int, default=0, help="If >0, only process first N selected rows (testing).")

    # Output target
    parser.add_argument("--storage", choices=["local", "gcs"], required=True)
    parser.add_argument("--out-dir", default="_tmp_10k_html", help="Local output root directory (for --storage local).")
    parser.add_argument("--gcs-bucket", help="GCS bucket name (for --storage gcs).")
    parser.add_argument("--gcs-prefix", help="GCS prefix/root folder (for --storage gcs).")

    # Runtime behavior
    parser.add_argument("--skip-if-exists", action="store_true", help="Skip if output object/file already exists.")
    parser.add_argument("--sleep", type=float, default=0.25, help="Sleep between filings to respect SEC rate limiting.")
    parser.add_argument("--max-retries", type=int, default=5, help="Max retries per SEC request.")
    args = parser.parse_args()

    # Validate output args
    if args.storage == "gcs":
        if not args.gcs_bucket or not args.gcs_prefix:
            raise ValueError("For --storage gcs, you must pass --gcs-bucket and --gcs-prefix")

    # Load dataset
    df = load_idx_df(args.input)

    required_cols = {"Filename", "Form Type", "CIK", "Year"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Input missing required columns: {sorted(missing)}")

    # Normalize key fields
    df = df.copy()
    df["Form Type"] = df["Form Type"].astype("string").str.strip().str.upper()
    df["CIK"] = df["CIK"].astype("string").map(normalize_cik)
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")

    # Always restrict to 10-K / 10-K/A for this script
    df = df[df["Form Type"].isin(list(VALID_TYPES))]

    # Apply selection filters unless --all
    if not args.all:
        cik_set: Set[str] = set()
        if args.cik:
            cik_set.add(normalize_cik(args.cik))
        ciks = parse_csv_list(args.ciks)
        if ciks:
            cik_set |= {normalize_cik(x) for x in ciks}

        year_set: Set[int] = set()
        if args.year is not None:
            year_set.add(int(args.year))
        years = parse_csv_list(args.years)
        if years:
            year_set |= {int(y) for y in years}

        if cik_set:
            df = df[df["CIK"].isin(list(cik_set))]
        if year_set:
            df = df[df["Year"].isin(list(year_set))]

    if df.empty:
        print("[WARN] No rows selected after filters. Nothing to do.")
        return

    # Optional small test run
    if args.limit and args.limit > 0:
        df = df.head(args.limit)

    print(f"[INFO] Rows selected: {len(df):,}")
    print(f"[INFO] Unique CIKs: {df['CIK'].nunique():,}")
    print(f"[INFO] Years: {sorted([int(x) for x in df['Year'].dropna().unique()])[:10]}{'...' if df['Year'].nunique() > 10 else ''}")

    session = requests.Session()

    ok = 0
    skipped = 0
    fail = 0

    # Iterate
    for _, row in df.iterrows():
        cik = str(row["CIK"])
        year = str(int(row["Year"])) if pd.notna(row["Year"]) else "unknown"

        # accession derived from filename for stable naming
        _, _, accession = build_index_url_from_filename(row["Filename"])
        accession = accession or "unknown"

        rel = build_output_relpath(cik, year, accession)

        if args.storage == "local":
            out_root = Path(args.out_dir).expanduser().resolve()
            out_path = out_root / rel
            if args.skip_if_exists and out_path.exists():
                skipped += 1
                continue
        else:
            bucket = args.gcs_bucket
            prefix = args.gcs_prefix.strip("/")
            obj = f"{prefix}/{rel}"
            if args.skip_if_exists and gcs_exists(bucket, obj):
                skipped += 1
                continue

        filing_url, html, rcik, racc = fetch_10k_html_for_row(
            session=session,
            row=row,
            user_agent=args.user_agent,
            max_retries=args.max_retries,
        )

        if not html:
            fail += 1
            time.sleep(args.sleep)
            continue

        gz = gzip_text(html)

        # Write
        if args.storage == "local":
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(gz)
        else:
            gcs_upload_bytes(args.gcs_bucket, obj, gz)

        ok += 1
        if ok % 50 == 0:
            print(f"[INFO] ok={ok:,} skipped={skipped:,} fail={fail:,} (latest: {filing_url})")

        time.sleep(args.sleep)

    print(f"[DONE] ok={ok:,} skipped={skipped:,} fail={fail:,}")


if __name__ == "__main__":
    main()
