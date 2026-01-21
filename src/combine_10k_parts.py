from __future__ import annotations

from io import BytesIO
from typing import List, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs

from src.common.imports import argparse


def parse_gcs_url(gs_url: str) -> Tuple[str, str]:
    s = gs_url.strip()
    if not s.startswith("gs://"):
        raise ValueError(f"Not a GCS URL: {gs_url}")
    rest = s[5:]
    parts = rest.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) == 2 else ""
    return bucket, key


def list_gcs_parquet(gs_prefix: str) -> List[str]:
    bucket, prefix = parse_gcs_url(gs_prefix)
    fs = pafs.GcsFileSystem()
    selector = pafs.FileSelector(f"{bucket}/{prefix}".rstrip("/") + "/", recursive=True)
    infos = fs.get_file_info(selector)
    paths = []
    for info in infos:
        if info.type == pafs.FileType.File and info.path.lower().endswith(".parquet"):
            # info.path is like "bucket/prefix/file.parquet"
            paths.append(info.path)
    paths.sort()
    return paths


def main():
    parser = argparse.ArgumentParser(description="Combine many 10-K parquet part files into a single parquet file (GCS->GCS).")
    parser.add_argument("--input-prefix", required=True, help="GCS prefix containing part-*.parquet, e.g. gs://bucket/path/to/parts")
    parser.add_argument("--output", required=True, help="Output parquet object, e.g. gs://bucket/path/to/company_index_10k.parquet")
    parser.add_argument("--compression", default="snappy", help="Parquet compression (default: snappy)")
    args = parser.parse_args()

    in_bucket, _ = parse_gcs_url(args.input_prefix)
    out_bucket, out_key = parse_gcs_url(args.output)

    if in_bucket != out_bucket:
        print("[WARN] Input and output buckets differ; that's fine, just confirming you intended this.")

    fs = pafs.GcsFileSystem()

    files = list_gcs_parquet(args.input_prefix)
    if not files:
        raise SystemExit(f"No parquet files found under {args.input_prefix}")

    print(f"[INFO] Found {len(files):,} parquet part files")

    # Open output stream and write incrementally
    out_path = f"{out_bucket}/{out_key}"
    with fs.open_output_stream(out_path) as out_stream:
        writer = None
        total_rows = 0

        for i, full_path in enumerate(files, start=1):
            # full_path is "bucket/prefix/part-xxxx.parquet"
            with fs.open_input_file(full_path) as f:
                table = pq.read_table(f)

            if writer is None:
                writer = pq.ParquetWriter(out_stream, table.schema, compression=args.compression)

            writer.write_table(table)
            total_rows += table.num_rows

            if i % 25 == 0 or i == len(files):
                print(f"[INFO] appended {i:,}/{len(files):,} files | rows so far: {total_rows:,}")

        if writer is not None:
            writer.close()

    print(f"[OK] Wrote combined parquet -> {args.output}")
    print(f"[OK] Total rows: {total_rows:,}")


if __name__ == "__main__":
    main()
