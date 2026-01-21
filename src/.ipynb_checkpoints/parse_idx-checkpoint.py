"""
Parse SEC EDGAR company.idx files into a single structured dataset.

Supports:
- Input from local directory (containing *.idx)
- Input from GCS prefix (gs://bucket/prefix containing *.idx)

Outputs:
- CSV or Parquet (recommended: Parquet)

Run examples:
  uv run python -m src.parse_company_idx \
    --input gs://sec-financials-edgar/idx_files \
    --output gs://sec-financials-edgar/idx_parsed/company_index.parquet \
    --format parquet

  uv run python -m src.parse_company_idx \
    --input /path/to/local/idx_files \
    --output /path/to/out/company_index.csv \
    --format csv
"""

from __future__ import annotations

import re
from io import BytesIO
from pathlib import Path
from typing import Iterable, Optional, Tuple

import pandas as pd

from src.common.imports import argparse, requests, require_gcs  # requests not used here, kept consistent with common
from src.common import config


COLSPECS = [(0, 62), (62, 74), (74, 86), (86, 98), (98, None)]
COLUMN_NAMES = ["Company Name", "Form Type", "CIK", "Date Filed", "Filename"]

IDX_NAME_RE = re.compile(r"(?P<year>\d{4})_(?P<qtr>QTR[1-4])_company\.idx$", re.IGNORECASE)


def is_gcs_path(p: str) -> bool:
    return p.strip().lower().startswith("gs://")


def parse_gcs_url(gs_url: str) -> Tuple[str, str]:
    """
    Parse gs://bucket/prefix into (bucket, prefix_without_leading_slash)
    """
    s = gs_url.strip()
    if not s.startswith("gs://"):
        raise ValueError(f"Not a GCS URL: {gs_url}")
    rest = s[5:]
    parts = rest.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) == 2 else ""
    prefix = prefix.strip("/")
    return bucket, prefix


def extract_year_quarter_from_name(name: str) -> Tuple[Optional[int], Optional[str]]:
    m = IDX_NAME_RE.search(name)
    if not m:
        return None, None
    return int(m.group("year")), m.group("qtr").upper()


# def read_single_idx_bytes(idx_bytes: bytes, source_name: str) -> pd.DataFrame:
#     """
#     Read a single .idx file (bytes) into a DataFrame using fixed-width specs.
#     """
#     # company.idx files are text; decode safely
#     text = idx_bytes.decode("latin-1", errors="replace")

#     # Use BytesIO for pandas; but read_fwf wants file-like with bytes or str.
#     # We'll feed it via StringIO approach using pandas built-in behavior on buffer.
#     buf = BytesIO(text.encode("utf-8", errors="ignore"))

#     df = pd.read_fwf(
#         buf,
#         colspecs=COLSPECS,
#         skiprows=9,
#         names=COLUMN_NAMES,
#         dtype=str,
#     )

#     # Clean columns
#     df.columns = [c.strip() for c in df.columns]
#     for c in df.columns:
#         df[c] = df[c].astype(str).str.strip()

#     # Drop obvious garbage rows (some idx files include blank lines)
#     df = df[df["Filename"].notna() & (df["Filename"].str.len() > 0)].copy()

#     year, qtr = extract_year_quarter_from_name(source_name)
#     df["Year"] = year
#     df["Quarter"] = qtr
#     df["SourceFile"] = source_name

#     return df
# Robust row parser:
# Company Name can contain spaces; fields are separated by 2+ spaces in idx files.
# Right-side tokens (Form Type, CIK, Date Filed, Filename) are stable.
ROW_RE = re.compile(
    r"^(?P<company_name>.+?)\s{2,}"
    r"(?P<form_type>\S+)\s+"
    r"(?P<cik>\d+)\s+"
    r"(?P<date_filed>\d{4}-\d{2}-\d{2})\s+"
    r"(?P<filename>.+)$"
)

def read_single_idx_bytes(idx_bytes: bytes, source_name: str) -> pd.DataFrame:
    text = idx_bytes.decode("latin-1", errors="replace")
    lines = text.splitlines()

    rows = []
    for line in lines:
        line = line.rstrip("\n")

        if not line.strip():
            continue
        if line.startswith("-----"):
            continue
        if line.lower().startswith("company name"):
            continue

        m = ROW_RE.match(line)
        if not m:
            continue

        rows.append(m.groupdict())

    if not rows:
        return pd.DataFrame()

    # ✅ df is created here
    df = pd.DataFrame(rows)

    # ✅ rename happens here — df now exists
    df = df.rename(columns={
        "company_name": "Company Name",
        "form_type": "Form Type",
        "cik": "CIK",
        "date_filed": "Date Filed",
        "filename": "Filename",
    })

    # Strip whitespace safely
    for c in df.columns:
        df[c] = df[c].astype("string").str.strip()

    year, qtr = extract_year_quarter_from_name(source_name)
    df["Year"] = year
    df["Quarter"] = qtr
    df["SourceFile"] = source_name

    return df


def iter_local_idx_files(input_dir: Path) -> Iterable[Path]:
    return sorted(input_dir.glob("*.idx"))


def load_from_local_directory(input_dir: str) -> pd.DataFrame:
    input_path = Path(input_dir).expanduser().resolve()
    if not input_path.exists() or not input_path.is_dir():
        raise ValueError(f"Local input directory not found: {input_path}")

    dfs = []
    for fp in iter_local_idx_files(input_path):
        try:
            idx_bytes = fp.read_bytes()
            dfs.append(read_single_idx_bytes(idx_bytes, fp.name))
        except Exception as e:
            print(f"[WARN] Failed to read {fp.name}: {type(e).__name__}: {e}")

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def load_from_gcs_prefix(gs_prefix: str) -> pd.DataFrame:
    """
    List *.idx objects in a GCS prefix and parse all of them.
    """
    require_gcs()
    from google.cloud import storage

    bucket_name, prefix = parse_gcs_url(gs_prefix)
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Ensure prefix ends without trailing slash; list_blobs will include children
    blobs = client.list_blobs(bucket_or_name=bucket_name, prefix=prefix)

    dfs = []
    n = 0
    for blob in blobs:
        name = blob.name
        if not name.lower().endswith(".idx"):
            continue

        source_name = Path(name).name
        try:
            idx_bytes = blob.download_as_bytes()
            dfs.append(read_single_idx_bytes(idx_bytes, source_name))
            n += 1
            if n % 25 == 0:
                print(f"[INFO] Parsed {n} idx files...")
        except Exception as e:
            print(f"[WARN] Failed to read gs://{bucket_name}/{name}: {type(e).__name__}: {e}")

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def output_exists_gcs(gs_url: str) -> bool:
    require_gcs()
    from google.cloud import storage

    bucket, prefix = parse_gcs_url(gs_url)
    client = storage.Client()

    # prefix here is actually full object path for output file
    blob = client.bucket(bucket).blob(prefix)
    return blob.exists(client)


def write_output(df: pd.DataFrame, output: str, fmt: str) -> None:
    fmt = fmt.lower().strip()
    if fmt not in ("csv", "parquet"):
        raise ValueError("--format must be one of: csv, parquet")

    if is_gcs_path(output):
        require_gcs()
        from google.cloud import storage

        bucket_name, object_path = parse_gcs_url(output)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_path)

        if fmt == "csv":
            data = df.to_csv(index=False).encode("utf-8")
            blob.upload_from_string(data, content_type="text/csv")
        else:
            # Parquet -> bytes buffer
            import io

            buf = io.BytesIO()
            df.to_parquet(buf, index=False)
            buf.seek(0)
            blob.upload_from_file(buf, content_type="application/octet-stream")

        print(f"[OK] Wrote {fmt.upper()} to {output}")
        return

    # Local output
    out_path = Path(output).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "csv":
        df.to_csv(out_path, index=False)
    else:
        df.to_parquet(out_path, index=False)

    print(f"[OK] Wrote {fmt.upper()} to {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Parse SEC EDGAR company.idx files into a structured dataset.")
    parser.add_argument(
        "--input",
        required=True,
        help="Local directory containing *.idx OR GCS prefix like gs://bucket/prefix",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output path (local file path OR gs://bucket/object).",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "parquet"],
        default="parquet",
        help="Output format. Default: parquet",
    )
    parser.add_argument(
        "--skip-if-exists",
        action="store_true",
        help="If output already exists, exit without doing work.",
    )

    args = parser.parse_args()

    # Optional idempotency for output artifact
    if args.skip_if_exists:
        if is_gcs_path(args.output):
            if output_exists_gcs(args.output):
                print(f"[SKIP] Output already exists: {args.output}")
                return
        else:
            if Path(args.output).expanduser().exists():
                print(f"[SKIP] Output already exists: {args.output}")
                return

    # Load
    if is_gcs_path(args.input):
        print(f"[INFO] Reading IDX files from GCS: {args.input}")
        df = load_from_gcs_prefix(args.input)
    else:
        print(f"[INFO] Reading IDX files from local directory: {args.input}")
        df = load_from_local_directory(args.input)

    if df.empty:
        print("[WARN] No rows parsed. Check input path/prefix.")
        return

    # Light cleanup: normalize key columns
    # CIK should be digits (but keep as string to preserve leading zeros if any)
    df["CIK"] = df["CIK"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()

    # Date parsing optional: keep as string for now (safe for downstream)
    # If you want datetime later, do it in the modeling/ETL stage.

    print(f"[INFO] Parsed rows: {len(df):,}")
    print(f"[INFO] Unique idx sources: {df['SourceFile'].nunique():,}")

    # Write output
    write_output(df, args.output, args.format)


if __name__ == "__main__":
    main()
