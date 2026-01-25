from __future__ import annotations

from typing import List, Tuple, Optional

import pyarrow.parquet as pq
import pyarrow.fs as pafs

import argparse  


FORM_PRESETS = {
    "10k": {"10-K", "10-K/A"},
    "8k": {"8-K", "8-K/A"},
    "10q": {"10-Q", "10-Q/A"},
}


def parse_gcs_url(gs_url: str) -> Tuple[str, str]:
    s = gs_url.strip()
    if not s.startswith("gs://"):
        raise ValueError(f"Not a GCS URL: {gs_url}")
    rest = s[5:]
    bucket, _, key = rest.partition("/")
    return bucket, key


def list_gcs_parquet_files(fs: pafs.FileSystem, gs_prefix: str) -> List[str]:
    """
    Returns file paths like 'bucket/prefix/part-000000.parquet' (no gs:// prefix)
    """
    bucket, prefix = parse_gcs_url(gs_prefix)
    selector = pafs.FileSelector(f"{bucket}/{prefix}".rstrip("/") + "/", recursive=True)
    infos = fs.get_file_info(selector)

    paths: List[str] = []
    for info in infos:
        if info.type == pafs.FileType.File and info.path.lower().endswith(".parquet"):
            paths.append(info.path)

    paths.sort()
    return paths


def forms_parts_folder(forms_arg: str, dataset_name: str) -> str:
    """
    Must match your split step folder naming:
      company_index_8k_parts, company_index_10k_parts, etc.
    """
    raw = (forms_arg or "").strip().lower()
    if raw in FORM_PRESETS:
        return f"{dataset_name}_{raw}_parts"
    return f"{dataset_name}_custom_parts"


def combine_parts_to_single_parquet(
    fs: pafs.FileSystem,
    input_prefix: str,
    output_uri: str,
    *,
    compression: str = "snappy",
    progress_every: int = 25,
) -> int:
    out_bucket, out_key = parse_gcs_url(output_uri)
    out_path = f"{out_bucket}/{out_key}"

    files = list_gcs_parquet_files(fs, input_prefix)
    if not files:
        raise SystemExit(f"No parquet files found under {input_prefix}")

    print(f"[INFO] Found {len(files):,} parquet files under {input_prefix}")
    print(f"[INFO] Writing output to {output_uri}")

    with fs.open_output_stream(out_path) as out_stream:
        writer: Optional[pq.ParquetWriter] = None
        total_rows = 0

        for i, full_path in enumerate(files, start=1):
            with fs.open_input_file(full_path) as f:
                table = pq.read_table(f)

            if writer is None:
                writer = pq.ParquetWriter(out_stream, table.schema, compression=compression)

            writer.write_table(table)
            total_rows += table.num_rows

            if i % progress_every == 0 or i == len(files):
                print(f"[INFO] appended {i:,}/{len(files):,} files | rows so far: {total_rows:,}")

        if writer is not None:
            writer.close()

    print(f"[OK] Wrote combined parquet -> {output_uri}")
    print(f"[OK] Total rows: {total_rows:,}")
    return total_rows


def main():
    parser = argparse.ArgumentParser(
        description="Combine split parquet parts (by filing type) into a single parquet file (GCS -> GCS)."
    )
    parser.add_argument(
        "--parts-root",
        required=True,
        help="GCS root folder containing parts folders, e.g. gs://bucket/edgar_idx_files/filtered",
    )
    parser.add_argument(
        "--dataset-name",
        default="company_index",
        help="Used to locate parts folder like company_index_8k_parts (default: company_index)",
    )
    parser.add_argument(
        "--forms",
        required=True,
        help="Preset key (10k, 8k, 10q) OR custom list (only affects folder naming: custom).",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional explicit output gs:// URI. If omitted, writes to <parts-root>/<dataset>_<forms>.parquet",
    )
    parser.add_argument("--compression", default="snappy", help="Parquet compression (default: snappy)")
    parser.add_argument("--progress-every", type=int, default=25, help="Log progress every N files (default: 25)")
    args = parser.parse_args()

    fs = pafs.GcsFileSystem()

    parts_folder = forms_parts_folder(args.forms, args.dataset_name)
    input_prefix = f"{args.parts_root.rstrip('/')}/{parts_folder}"

    # default output name (you removed _merged — that's fine)
    if args.output.strip():
        output_uri = args.output.strip()
    else:
        forms_key = args.forms.strip().lower()
        forms_key = forms_key if forms_key in FORM_PRESETS else "custom"
        output_uri = f"{args.parts_root.rstrip('/')}/{args.dataset_name}_{forms_key}.parquet"

    print(f"[INFO] Parts root: {args.parts_root}")
    print(f"[INFO] Parts folder: {parts_folder}")
    print(f"[INFO] Input prefix: {input_prefix}")
    print(f"[INFO] Output uri: {output_uri}")

    combine_parts_to_single_parquet(
        fs,
        input_prefix=input_prefix,
        output_uri=output_uri,
        compression=args.compression,
        progress_every=args.progress_every,
    )


if __name__ == "__main__":
    main()