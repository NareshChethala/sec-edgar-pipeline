from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs

from src.common.imports import argparse


TENK_FORMS = {"10-K", "10-K/A"}


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


def clean_and_filter_10k(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize form type
    df["Form Type"] = df["Form Type"].astype("string").str.strip().str.upper()

    # Keep only 10-K / 10-K/A early (saves work)
    df = df[df["Form Type"].isin(TENK_FORMS)].copy()
    if df.empty:
        return df

    # Clean Date Filed and parse safely
    df["Date Filed"] = df["Date Filed"].astype("string").str.strip()
    df["Date Filed_dt"] = pd.to_datetime(df["Date Filed"], errors="coerce", format="mixed")
    df = df[df["Date Filed_dt"].notna()].copy()
    if df.empty:
        return df

    # Add Year (useful for partitions/filters later)
    df["Year"] = df["Date Filed_dt"].dt.year.astype("int32")

    # Normalize stored Date Filed string
    df["Date Filed"] = df["Date Filed_dt"].dt.strftime("%Y-%m-%d")

    df.drop(columns=["Date Filed_dt"], inplace=True)
    return df


def make_output_path(base: str, storage: str, bucket: Optional[str], part: int) -> str:
    # always write multiple parts, predictable naming
    filename = f"part-{part:06d}.parquet"

    if storage == "local":
        out_dir = Path(base).expanduser().resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        return str(out_dir / filename)

    if not bucket:
        raise ValueError("--gcs-bucket is required for --storage gcs")

    prefix = base.strip("/")
    return f"gs://{bucket}/{prefix}/{filename}"


def write_parquet_any(df: pd.DataFrame, out_path: str) -> None:
    if is_gcs_path(out_path):
        # Write to GCS using PyArrow filesystem (stream upload)
        bucket, key = parse_gcs_url(out_path)
        fs = pafs.GcsFileSystem()  # uses ADC on VM
        table = pa.Table.from_pandas(df, preserve_index=False)

        with fs.open_output_stream(f"{bucket}/{key}") as f:
            pq.write_table(table, f, compression="snappy")
        return

    # Local
    df.to_parquet(out_path, index=False, engine="pyarrow")


def stream_parquet_rowgroups_gcs(input_path: str):
    """
    Stream Parquet row-groups WITHOUT downloading entire file.
    Works for both local and gs:// paths.
    """
    if is_gcs_path(input_path):
        bucket, key = parse_gcs_url(input_path)
        fs = pafs.GcsFileSystem()
        # ParquetFile can read from a file-like opened via filesystem
        f = fs.open_input_file(f"{bucket}/{key}")
        pf = pq.ParquetFile(f)
        return pf, f  # keep f alive
    else:
        pf = pq.ParquetFile(Path(input_path).expanduser().resolve())
        return pf, None


def main():
    parser = argparse.ArgumentParser(
        description="Stream a huge dataset in batches, filter 10-K/10-K/A, write output in batches."
    )
    parser.add_argument("--input", required=True, help="Input file: .parquet or .csv (local or gs:// for parquet).")
    parser.add_argument("--input-format", choices=["parquet", "csv"], default="parquet")
    parser.add_argument("--storage", choices=["local", "gcs"], required=True, help="Where to write output parts.")
    parser.add_argument(
        "--out-base",
        required=True,
        help="Output folder/prefix (local folder OR GCS prefix inside bucket).",
    )
    parser.add_argument("--gcs-bucket", default=None, help="GCS bucket name (required if --storage gcs).")
    parser.add_argument(
        "--csv-chunksize",
        type=int,
        default=250_000,
        help="Rows per chunk for CSV input (default 250k).",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=0,
        help="For testing: stop after N batches (0 = all).",
    )
    args = parser.parse_args()

    part = 0
    total_in = 0
    total_out = 0

    if args.input_format == "csv":
        # CSV streaming (local or gs:// only if your env supports fsspec/gcsfs)
        for i, chunk in enumerate(pd.read_csv(args.input, chunksize=args.csv_chunksize, dtype=str)):
            if args.max_batches and i >= args.max_batches:
                break

            total_in += len(chunk)
            out_df = clean_and_filter_10k(chunk)
            total_out += len(out_df)

            print(f"[BATCH {i+1:04d}] in={len(chunk):,} out(10-K)={len(out_df):,}")

            if not out_df.empty:
                out_path = make_output_path(args.out_base, args.storage, args.gcs_bucket, part)
                write_parquet_any(out_df, out_path)
                print(f"[OK] wrote {len(out_df):,} -> {out_path}")
                part += 1

    else:
        # Parquet streaming via row groups (best for huge parquet)
        pf, handle = stream_parquet_rowgroups_gcs(args.input)
        try:
            for rg in range(pf.num_row_groups):
                if args.max_batches and rg >= args.max_batches:
                    break

                table = pf.read_row_group(rg)
                chunk = table.to_pandas(types_mapper=pd.ArrowDtype)  # memory friendlier dtypes
                total_in += len(chunk)

                out_df = clean_and_filter_10k(chunk)
                total_out += len(out_df)

                print(f"[ROWGROUP {rg+1:04d}/{pf.num_row_groups}] in={len(chunk):,} out(10-K)={len(out_df):,}")

                if not out_df.empty:
                    out_path = make_output_path(args.out_base, args.storage, args.gcs_bucket, part)
                    write_parquet_any(out_df, out_path)
                    print(f"[OK] wrote {len(out_df):,} -> {out_path}")
                    part += 1
        finally:
            if handle is not None:
                handle.close()

    print("------------------------------------------------------------")
    print(f"[DONE] total input rows: {total_in:,}")
    print(f"[DONE] total 10-K rows:  {total_out:,}")
    print(f"[DONE] parts written:    {part:,}")


if __name__ == "__main__":
    main()
