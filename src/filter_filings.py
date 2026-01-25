from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple, Set

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs

from src.common.imports import argparse


# Built-in presets (extend anytime)
FORM_PRESETS: dict[str, set[str]] = {
    "10k": {"10-K", "10-K/A"},
    "8k": {"8-K", "8-K/A"},
    "10q": {"10-Q", "10-Q/A"},
}


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


# def forms_output_subdir(forms_arg: str) -> str:
#     """
#     Output subfolder name based on --forms.
#     Presets use their preset key (10k, 8k, 10q).
#     Custom lists go under 'custom'.
#     """
#     raw = (forms_arg or "").strip().lower()
#     return raw if raw in FORM_PRESETS else "custom"
def forms_output_subdir(forms_arg: str) -> str:
    """
    Output subfolder name like:
    company_index_8k_parts
    company_index_10k_parts
    company_index_custom_parts
    """
    raw = (forms_arg or "").strip().lower()
    if raw in FORM_PRESETS:
        return f"company_index_{raw}_parts"
    return "company_index_custom_parts"


def parse_forms_arg(forms_arg: str) -> Set[str]:
    """
    Accepts:
      - preset name: "10k" or "8k" or "10q"
      - custom list: "10-K,10-K/A" or "8-K" or "S-1,S-1/A"
    Returns:
      - set of normalized form types in UPPER CASE
    """
    if not forms_arg or not forms_arg.strip():
        raise ValueError("--forms is required (e.g., --forms 10k OR --forms '10-K,10-K/A').")

    raw = forms_arg.strip().lower()

    # preset
    if raw in FORM_PRESETS:
        return {f.strip().upper() for f in FORM_PRESETS[raw]}

    # custom comma-separated list
    parts = [p.strip() for p in forms_arg.split(",") if p.strip()]
    if not parts:
        raise ValueError(
            "Invalid --forms. Use a preset (10k, 8k, 10q) or a comma-separated list like '10-K,10-K/A'."
        )
    return {p.upper() for p in parts}


def clean_and_filter_forms(df: pd.DataFrame, allowed_forms: Set[str]) -> pd.DataFrame:
    """
    Keeps only rows with Form Type in allowed_forms.
    Cleans/parses Date Filed and adds Year.
    """
    if "Form Type" not in df.columns:
        raise KeyError("Expected column 'Form Type' in input data.")
    if "Date Filed" not in df.columns:
        raise KeyError("Expected column 'Date Filed' in input data.")

    # Normalize form type
    df["Form Type"] = df["Form Type"].astype("string").str.strip().str.upper()

    # Keep only selected forms early
    df = df[df["Form Type"].isin(allowed_forms)].copy()
    if df.empty:
        return df

    # Clean Date Filed and parse safely
    df["Date Filed"] = df["Date Filed"].astype("string").str.strip()
    df["Date Filed_dt"] = pd.to_datetime(df["Date Filed"], errors="coerce", format="mixed")
    df = df[df["Date Filed_dt"].notna()].copy()
    if df.empty:
        return df

    # Add Year
    df["Year"] = df["Date Filed_dt"].dt.year.astype("int32")

    # Normalize stored Date Filed string
    df["Date Filed"] = df["Date Filed_dt"].dt.strftime("%Y-%m-%d")

    df.drop(columns=["Date Filed_dt"], inplace=True)
    return df


def make_output_path(
    base: str,
    storage: str,
    bucket: Optional[str],
    part: int,
    subdir: str,
) -> str:
    filename = f"part-{part:06d}.parquet"

    if storage == "local":
        out_dir = Path(base).expanduser().resolve() / subdir
        out_dir.mkdir(parents=True, exist_ok=True)
        return str(out_dir / filename)

    if not bucket:
        raise ValueError("--gcs-bucket is required for --storage gcs")

    prefix = f"{base.strip('/')}/{subdir}"
    return f"gs://{bucket}/{prefix}/{filename}"


def write_parquet_any(df: pd.DataFrame, out_path: str) -> None:
    if is_gcs_path(out_path):
        bucket, key = parse_gcs_url(out_path)
        fs = pafs.GcsFileSystem()  # uses ADC on VM
        table = pa.Table.from_pandas(df, preserve_index=False)

        with fs.open_output_stream(f"{bucket}/{key}") as f:
            pq.write_table(table, f, compression="snappy")
        return

    df.to_parquet(out_path, index=False, engine="pyarrow")


def stream_parquet_rowgroups_gcs(input_path: str):
    """
    Stream Parquet row-groups WITHOUT downloading entire file.
    Works for both local and gs:// paths.
    """
    if is_gcs_path(input_path):
        bucket, key = parse_gcs_url(input_path)
        fs = pafs.GcsFileSystem()
        f = fs.open_input_file(f"{bucket}/{key}")
        pf = pq.ParquetFile(f)
        return pf, f  # keep f alive
    else:
        pf = pq.ParquetFile(Path(input_path).expanduser().resolve())
        return pf, None


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Stream a huge dataset in batches, filter by SEC form types, write output parquet parts.\n"
            "Examples:\n"
            "  --forms 10k\n"
            "  --forms 8k\n"
            "  --forms '10-K,10-K/A,8-K'\n"
        )
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

    # User chooses which forms to keep
    parser.add_argument(
        "--forms",
        required=True,
        help="Preset (10k, 8k, 10q) OR custom comma list like '10-K,10-K/A'.",
    )

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
        help="For testing: stop after N batches/rowgroups (0 = all).",
    )
    args = parser.parse_args()

    allowed_forms = parse_forms_arg(args.forms)
    forms_subdir = forms_output_subdir(args.forms)

    print(f"[INFO] Filtering Form Type in: {sorted(allowed_forms)}")
    print(f"[INFO] Writing output under: {args.out_base.rstrip('/')}/{forms_subdir}/")

    part = 0
    total_in = 0
    total_out = 0

    if args.input_format == "csv":
        for i, chunk in enumerate(pd.read_csv(args.input, chunksize=args.csv_chunksize, dtype=str)):
            if args.max_batches and i >= args.max_batches:
                break

            total_in += len(chunk)
            out_df = clean_and_filter_forms(chunk, allowed_forms)
            total_out += len(out_df)

            print(f"[BATCH {i+1:04d}] in={len(chunk):,} out(filtered)={len(out_df):,}")

            if not out_df.empty:
                out_path = make_output_path(args.out_base, args.storage, args.gcs_bucket, part, forms_subdir)
                write_parquet_any(out_df, out_path)
                print(f"[OK] wrote {len(out_df):,} -> {out_path}")
                part += 1

    else:
        pf, handle = stream_parquet_rowgroups_gcs(args.input)
        try:
            for rg in range(pf.num_row_groups):
                if args.max_batches and rg >= args.max_batches:
                    break

                table = pf.read_row_group(rg)
                chunk = table.to_pandas(types_mapper=pd.ArrowDtype)
                total_in += len(chunk)

                out_df = clean_and_filter_forms(chunk, allowed_forms)
                total_out += len(out_df)

                print(f"[ROWGROUP {rg+1:04d}/{pf.num_row_groups}] in={len(chunk):,} out(filtered)={len(out_df):,}")

                if not out_df.empty:
                    out_path = make_output_path(args.out_base, args.storage, args.gcs_bucket, part, forms_subdir)
                    write_parquet_any(out_df, out_path)
                    print(f"[OK] wrote {len(out_df):,} -> {out_path}")
                    part += 1
        finally:
            if handle is not None:
                handle.close()

    print("------------------------------------------------------------")
    print(f"[DONE] total input rows:   {total_in:,}")
    print(f"[DONE] total output rows:  {total_out:,}")
    print(f"[DONE] parts written:      {part:,}")


if __name__ == "__main__":
    main()