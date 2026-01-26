#!/usr/bin/env python3
"""
Convert SEC Financial Statement Data Sets quarter folders from .txt (tab-delimited)
to .parquet, without transforming data.

Supports:
- Local paths:      /path/to/raw_txt
- GCS paths:        gs://bucket/path/raw_txt
- S3 paths:         s3://bucket/path/raw_txt

Assumes quarter folders contain: sub.txt, num.txt, tag.txt, pre.txt

Example:
  python -m src.pipelines.sec_txt_to_parquet \
    --input gs://my-bucket/sec_financials/raw_txt \
    --output gs://my-bucket/sec_financials/parquet

  python -m src.pipelines.sec_txt_to_parquet \
    --input s3://my-bucket/sec_financials/raw_txt \
    --output s3://my-bucket/sec_financials/parquet

  python -m src.pipelines.sec_txt_to_parquet \
    --input ./data/sec_financials/raw_txt \
    --output ./data/sec_financials/parquet
"""

from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd

# You said you already have src/imports.py and __init__.py
# and you previously used: from src.common.imports import argparse
# Keep it consistent with your project.
from src.common.imports import argparse  # type: ignore


FILES_DEFAULT = ["sub", "num", "tag", "pre"]
QUARTER_RE = re.compile(r"^\d{4}q[1-4]$", re.IGNORECASE)


# -----------------------------
# Storage helpers (local / gs:// / s3://)
# -----------------------------
def _strip_scheme(path: str) -> str:
    if path.startswith("gs://"):
        return path[5:]
    if path.startswith("s3://"):
        return path[5:]
    return path


def detect_backend(path: str) -> str:
    p = path.strip().lower()
    if p.startswith("gs://"):
        return "gcs"
    if p.startswith("s3://"):
        return "s3"
    return "local"


def make_fs(backend: str, *, anon: bool = False):
    """
    Returns an fsspec-compatible filesystem object.
    """
    if backend == "gcs":
        import gcsfs  # pip install gcsfs

        return gcsfs.GCSFileSystem(anon=anon)
    if backend == "s3":
        import s3fs  # pip install s3fs

        # If you're using AWS creds via env / config file, this just works.
        return s3fs.S3FileSystem(anon=anon)
    # local filesystem: use fsspec's local
    import fsspec

    return fsspec.filesystem("file")


def join_path(root: str, *parts: str) -> str:
    """
    Joins paths safely for local and cloud URIs.
    """
    root = root.rstrip("/")
    suffix = "/".join(p.strip("/").replace("\\", "/") for p in parts if p is not None)
    return f"{root}/{suffix}" if suffix else root


def ls_dirs(fs, root: str) -> List[str]:
    """
    Lists direct children under root. Returns fully qualified paths.
    For GCS/S3 we treat "directories" as prefixes.
    """
    # gcsfs/s3fs accept "gs://bucket/.." and "s3://bucket/.."
    # but their fs.ls() may return without scheme sometimes.
    items = fs.ls(root)

    out = []
    backend = detect_backend(root)

    for p in items:
        # Normalize to scheme paths for cloud.
        if backend == "gcs" and not str(p).startswith("gs://"):
            p = "gs://" + str(p)
        elif backend == "s3" and not str(p).startswith("s3://"):
            p = "s3://" + str(p)

        # Heuristic: treat as directory/prefix if it contains something.
        try:
            inner = fs.ls(p)
            if inner:
                out.append(str(p))
        except Exception:
            # ignore non-prefix / non-dir items
            pass

    return sorted(out)


def exists(fs, path: str) -> bool:
    return bool(fs.exists(path))


# -----------------------------
# Core conversion
# -----------------------------
def txt_to_parquet(
    fs,
    txt_path: str,
    parquet_path: str,
    *,
    overwrite: bool,
    force_all_strings: bool,
) -> Tuple[bool, Optional[int], Optional[int]]:
    """
    Returns (converted?, rows, cols)
    """
    if (not overwrite) and exists(fs, parquet_path):
        print(f"↩️  SKIP (exists): {parquet_path}")
        return (False, None, None)

    # Strictest no-coercion option: read everything as string
    read_kwargs = dict(sep="\t", low_memory=False)
    if force_all_strings:
        read_kwargs["dtype"] = str

    df = pd.read_csv(txt_path, **read_kwargs)

    # Write parquet back to same backend using fsspec file handle
    # (works reliably across local / gcs / s3).
    with fs.open(parquet_path, "wb") as f:
        df.to_parquet(f, engine="pyarrow", index=False)

    print(f"✔ Converted: {txt_path} → {parquet_path} | rows={len(df):,} cols={df.shape[1]}")
    return (True, int(len(df)), int(df.shape[1]))


def is_quarter_folder(path: str) -> bool:
    # last path component, ignoring trailing slash
    name = path.rstrip("/").split("/")[-1]
    return bool(QUARTER_RE.match(name))


def run(
    input_root: str,
    output_root: str,
    *,
    files: List[str],
    overwrite: bool,
    only_quarters: Optional[List[str]],
    skip_quarters: Optional[List[str]],
    anon: bool,
    force_all_strings: bool,
    dry_run: bool,
) -> None:
    backend = detect_backend(input_root)
    out_backend = detect_backend(output_root)
    if backend != out_backend:
        raise ValueError(
            f"Input backend ({backend}) != output backend ({out_backend}). "
            f"Use same backend for both input and output paths."
        )

    fs = make_fs(backend, anon=anon)

    # List quarter folders under input_root
    candidates = ls_dirs(fs, input_root)

    # Filter to quarter-like names if the input root is a parent folder
    quarter_paths = [p for p in candidates if is_quarter_folder(p)]
    if not quarter_paths:
        # If input_root itself is a quarter folder, accept it
        if is_quarter_folder(input_root) and exists(fs, input_root):
            quarter_paths = [input_root]
        else:
            # Otherwise convert all folders (some users store as year/quarter)
            quarter_paths = candidates

    # Apply include/exclude filters
    def qname(p: str) -> str:
        return p.rstrip("/").split("/")[-1]

    if only_quarters:
        allowed = {q.lower() for q in only_quarters}
        quarter_paths = [p for p in quarter_paths if qname(p).lower() in allowed]

    if skip_quarters:
        blocked = {q.lower() for q in skip_quarters}
        quarter_paths = [p for p in quarter_paths if qname(p).lower() not in blocked]

    print(f"Backend: {backend}")
    print(f"Input root:  {input_root}")
    print(f"Output root: {output_root}")
    print(f"Found {len(quarter_paths)} quarter folders to process.")

    for qpath in quarter_paths:
        quarter = qname(qpath)
        out_qpath = join_path(output_root, quarter)

        print("\n" + "=" * 90)
        print(f"[QUARTER] {quarter}")
        print("=" * 90)

        for f in files:
            txt_path = join_path(qpath, f"{f}.txt")
            parquet_path = join_path(out_qpath, f"{f}.parquet")

            if not exists(fs, txt_path):
                print(f"⚠️  MISSING: {txt_path} (skipping)")
                continue

            if dry_run:
                action = "OVERWRITE" if overwrite else "WRITE_IF_MISSING"
                print(f"🧪 DRY RUN: {action} {txt_path} -> {parquet_path}")
                continue

            txt_to_parquet(
                fs,
                txt_path,
                parquet_path,
                overwrite=overwrite,
                force_all_strings=force_all_strings,
            )

    print("\n✅ Done.")


# -----------------------------
# CLI
# -----------------------------
def build_parser():
    p = argparse.ArgumentParser(
        prog="sec_txt_to_parquet",
        description="Convert SEC financial statement quarterly .txt files (tab-delimited) to .parquet (lossless conversion).",
    )

    p.add_argument(
        "--input",
        required=True,
        help="Input root containing quarter folders (local, gs://, or s3://).",
    )
    p.add_argument(
        "--output",
        required=True,
        help="Output root where quarter folders and parquet files will be written (same backend as input).",
    )

    p.add_argument(
        "--files",
        default=",".join(FILES_DEFAULT),
        help="Comma-separated list of dataset basenames to convert (default: sub,num,tag,pre).",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing parquet files. Default is to skip if parquet exists.",
    )
    p.add_argument(
        "--only-quarters",
        default="",
        help="Comma-separated quarter folder names to include only (e.g., 2020q1,2020q2).",
    )
    p.add_argument(
        "--skip-quarters",
        default="",
        help="Comma-separated quarter folder names to skip (e.g., 2009q1).",
    )
    p.add_argument(
        "--anon",
        action="store_true",
        help="Anonymous access (rare; generally leave off).",
    )
    p.add_argument(
        "--force-all-strings",
        action="store_true",
        help="Read all TXT columns as strings (strictest 'no type coercion' mode).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen without converting/writing files.",
    )

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    files = [x.strip() for x in args.files.split(",") if x.strip()]
    only_quarters = [x.strip() for x in args.only_quarters.split(",") if x.strip()] or None
    skip_quarters = [x.strip() for x in args.skip_quarters.split(",") if x.strip()] or None

    run(
        input_root=args.input,
        output_root=args.output,
        files=files,
        overwrite=bool(args.overwrite),
        only_quarters=only_quarters,
        skip_quarters=skip_quarters,
        anon=bool(args.anon),
        force_all_strings=bool(args.force_all_strings),
        dry_run=bool(args.dry_run),
    )


if __name__ == "__main__":
    main()