#!/usr/bin/env python3
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple

import pandas as pd

from src.common.imports import argparse  # keep consistent with your repo


FILES_DEFAULT = ["sub", "num", "tag", "pre"]
QUARTER_RE = re.compile(r"^\d{4}q[1-4]$", re.IGNORECASE)


# -----------------------------
# Storage helpers (local / gs:// / s3://)
# -----------------------------
def detect_backend(path: str) -> str:
    p = path.strip().lower()
    if p.startswith("gs://"):
        return "gcs"
    if p.startswith("s3://"):
        return "s3"
    return "local"


def make_fs(backend: str, *, anon: bool = False):
    if backend == "gcs":
        import gcsfs  # pip install gcsfs

        return gcsfs.GCSFileSystem(anon=anon)
    if backend == "s3":
        import s3fs  # pip install s3fs

        return s3fs.S3FileSystem(anon=anon)
    import fsspec

    return fsspec.filesystem("file")


def join_path(root: str, *parts: str) -> str:
    root = root.rstrip("/")
    suffix = "/".join(p.strip("/").replace("\\", "/") for p in parts if p)
    return f"{root}/{suffix}" if suffix else root


def exists(fs, path: str) -> bool:
    return bool(fs.exists(path))


def ls_dirs(fs, root: str) -> List[str]:
    items = fs.ls(root)
    backend = detect_backend(root)
    out = []

    for p in items:
        p = str(p)
        if backend == "gcs" and not p.startswith("gs://"):
            p = "gs://" + p
        elif backend == "s3" and not p.startswith("s3://"):
            p = "s3://" + p

        try:
            inner = fs.ls(p)
            if inner:
                out.append(p)
        except Exception:
            pass

    return sorted(out)


def is_quarter_folder(path: str) -> bool:
    name = path.rstrip("/").split("/")[-1]
    return bool(QUARTER_RE.match(name))


# -----------------------------
# Strong content verification (optional)
# -----------------------------
def hash_df(df: pd.DataFrame) -> str:
    """
    Content hash that is stable across runs, but can be expensive for huge files.
    Uses pandas hashing; does NOT require reading both full files into memory
    at once (but still reads the df you pass in).
    """
    import hashlib

    # Include columns + values
    hcols = hashlib.sha256(("|".join(df.columns)).encode("utf-8")).hexdigest()
    hvals = pd.util.hash_pandas_object(df, index=False).values
    hvals_bytes = hvals.tobytes()
    hdata = hashlib.sha256(hvals_bytes).hexdigest()
    return f"{hcols}:{hdata}"


# -----------------------------
# Verification
# -----------------------------
@dataclass
class CheckResult:
    quarter: str
    table: str
    ok: bool
    reason: str
    txt_rows: Optional[int] = None
    txt_cols: Optional[int] = None
    pq_rows: Optional[int] = None
    pq_cols: Optional[int] = None


def verify_one_table(
    fs,
    txt_path: str,
    pq_path: str,
    *,
    table: str,
    quarter: str,
    force_all_strings: bool,
    strict_hash: bool,
) -> CheckResult:
    if not exists(fs, txt_path):
        return CheckResult(quarter, table, False, f"Missing TXT: {txt_path}")
    if not exists(fs, pq_path):
        return CheckResult(quarter, table, False, f"Missing Parquet: {pq_path}")

    # Read TXT
    read_kwargs = dict(sep="\t", low_memory=False)
    if force_all_strings:
        read_kwargs["dtype"] = str

    txt_df = pd.read_csv(txt_path, **read_kwargs)

    # Read Parquet via fs handle
    with fs.open(pq_path, "rb") as f:
        pq_df = pd.read_parquet(f, engine="pyarrow")

    # Columns
    if list(txt_df.columns) != list(pq_df.columns):
        return CheckResult(
            quarter,
            table,
            False,
            "Column mismatch (names/order)",
            txt_rows=len(txt_df),
            txt_cols=txt_df.shape[1],
            pq_rows=len(pq_df),
            pq_cols=pq_df.shape[1],
        )

    # Shapes
    if txt_df.shape != pq_df.shape:
        return CheckResult(
            quarter,
            table,
            False,
            "Shape mismatch (rows/cols)",
            txt_rows=len(txt_df),
            txt_cols=txt_df.shape[1],
            pq_rows=len(pq_df),
            pq_cols=pq_df.shape[1],
        )

    # Optional strict content check
    if strict_hash:
        h_txt = hash_df(txt_df)
        h_pq = hash_df(pq_df)
        if h_txt != h_pq:
            return CheckResult(
                quarter,
                table,
                False,
                "Content hash mismatch",
                txt_rows=len(txt_df),
                txt_cols=txt_df.shape[1],
                pq_rows=len(pq_df),
                pq_cols=pq_df.shape[1],
            )

    return CheckResult(
        quarter,
        table,
        True,
        "OK",
        txt_rows=len(txt_df),
        txt_cols=txt_df.shape[1],
        pq_rows=len(pq_df),
        pq_cols=pq_df.shape[1],
    )


def pick_quarters(fs, input_root: str, *, only: Optional[List[str]], skip: Optional[List[str]]) -> List[str]:
    candidates = ls_dirs(fs, input_root)
    quarters = [p for p in candidates if is_quarter_folder(p)]

    if not quarters:
        # if input_root itself is a quarter
        if is_quarter_folder(input_root) and exists(fs, input_root):
            quarters = [input_root]
        else:
            quarters = candidates

    def qname(p: str) -> str:
        return p.rstrip("/").split("/")[-1]

    if only:
        allowed = {q.lower() for q in only}
        quarters = [p for p in quarters if qname(p).lower() in allowed]

    if skip:
        blocked = {q.lower() for q in skip}
        quarters = [p for p in quarters if qname(p).lower() not in blocked]

    return sorted(quarters)


def run_verify(
    txt_root: str,
    parquet_root: str,
    *,
    files: List[str],
    only_quarters: Optional[List[str]],
    skip_quarters: Optional[List[str]],
    anon: bool,
    force_all_strings: bool,
    strict_hash: bool,
) -> int:
    backend_txt = detect_backend(txt_root)
    backend_pq = detect_backend(parquet_root)
    if backend_txt != backend_pq:
        raise ValueError("TXT root backend and Parquet root backend must match (both local or both gs:// or both s3://).")

    fs = make_fs(backend_txt, anon=anon)

    quarter_paths = pick_quarters(fs, txt_root, only=only_quarters, skip=skip_quarters)
    print(f"Backend: {backend_txt}")
    print(f"TXT root:     {txt_root}")
    print(f"Parquet root: {parquet_root}")
    print(f"Quarters to verify: {len(quarter_paths)}")
    print(f"Tables: {files}")
    print(f"Strict hash: {strict_hash}")

    failures = 0

    for qpath in quarter_paths:
        quarter = qpath.rstrip("/").split("/")[-1]
        print("\n" + "=" * 90)
        print(f"[VERIFY] {quarter}")
        print("=" * 90)

        for t in files:
            txt_path = join_path(qpath, f"{t}.txt")
            pq_path = join_path(parquet_root, quarter, f"{t}.parquet")

            res = verify_one_table(
                fs,
                txt_path,
                pq_path,
                table=t,
                quarter=quarter,
                force_all_strings=force_all_strings,
                strict_hash=strict_hash,
            )

            if res.ok:
                print(f"✅ {t}: {res.reason} | rows={res.txt_rows:,} cols={res.txt_cols}")
            else:
                failures += 1
                print(f"❌ {t}: {res.reason}")
                if res.txt_rows is not None:
                    print(f"   TXT: rows={res.txt_rows:,} cols={res.txt_cols}")
                if res.pq_rows is not None:
                    print(f"   PQ : rows={res.pq_rows:,} cols={res.pq_cols}")

    print("\n" + ("✅ ALL CHECKS PASSED" if failures == 0 else f"❌ FAILURES: {failures}"))
    return failures


# -----------------------------
# CLI
# -----------------------------
def build_parser():
    p = argparse.ArgumentParser(
        prog="sec_verify_txt_vs_parquet",
        description="Verify TXT vs Parquet conversion for SEC quarterly financial statement datasets.",
    )

    p.add_argument("--txt-root", required=True, help="Root folder containing quarter folders with .txt files (local/gs://s3://).")
    p.add_argument("--parquet-root", required=True, help="Root folder containing quarter folders with .parquet files (local/gs://s3://).")

    p.add_argument("--files", default=",".join(FILES_DEFAULT), help="Comma-separated tables to verify (default: sub,num,tag,pre).")
    p.add_argument("--only-quarters", default="", help="Comma-separated quarters to verify (e.g., 2020q1,2020q2).")
    p.add_argument("--skip-quarters", default="", help="Comma-separated quarters to skip.")
    p.add_argument("--anon", action="store_true", help="Anonymous access (usually off).")
    p.add_argument("--force-all-strings", action="store_true", help="Read TXT with dtype=str (strictest no-coercion mode).")
    p.add_argument("--strict-hash", action="store_true", help="Also compare content hashes (slow for big NUM).")

    return p


def main():
    args = build_parser().parse_args()

    files = [x.strip() for x in args.files.split(",") if x.strip()]
    only = [x.strip() for x in args.only_quarters.split(",") if x.strip()] or None
    skip = [x.strip() for x in args.skip_quarters.split(",") if x.strip()] or None

    failures = run_verify(
        txt_root=args.txt_root,
        parquet_root=args.parquet_root,
        files=files,
        only_quarters=only,
        skip_quarters=skip,
        anon=bool(args.anon),
        force_all_strings=bool(args.force_all_strings),
        strict_hash=bool(args.strict_hash),
    )

    raise SystemExit(0 if failures == 0 else 2)


if __name__ == "__main__":
    main()
