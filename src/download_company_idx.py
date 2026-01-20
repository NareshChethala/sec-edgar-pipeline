# code/download_company_idx.py

from src.common.imports import argparse, time, Path, requests, require_gcs, require_s3
from src.common import config


def download_file_to_local(url, local_path, headers):
    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)


def exists_local(out_dir, filename):
    return (Path(out_dir).expanduser() / filename).exists()


def exists_gcs(bucket_name, prefix, filename):
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    prefix = (prefix or "").strip("/")
    blob_name = f"{prefix}/{filename}" if prefix else filename

    blob = bucket.blob(blob_name)
    return blob.exists(client)


def exists_s3(bucket_name, prefix, filename, region=None):
    import boto3

    s3 = boto3.client("s3", region_name=region) if region else boto3.client("s3")

    prefix = (prefix or "").strip("/")
    key = f"{prefix}/{filename}" if prefix else filename

    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except Exception as e:
        if hasattr(e, "response") and isinstance(e.response, dict):
            code = str(e.response.get("Error", {}).get("Code", ""))
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
        msg = str(e).lower()
        if "404" in msg or "not found" in msg or "no such key" in msg:
            return False
        raise


def upload_to_gcs(local_path, bucket_name, prefix):
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    prefix = (prefix or "").strip("/")
    blob_name = f"{prefix}/{local_path.name}" if prefix else local_path.name

    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_path))


def upload_to_s3(local_path, bucket_name, prefix, region=None):
    import boto3

    s3 = boto3.client("s3", region_name=region) if region else boto3.client("s3")

    prefix = (prefix or "").strip("/")
    key = f"{prefix}/{local_path.name}" if prefix else local_path.name

    s3.upload_file(str(local_path), bucket_name, key)


def parse_quarters(quarters_arg):
    all_quarters = ["QTR1", "QTR2", "QTR3", "QTR4"]
    if not quarters_arg:
        return all_quarters
    q = quarters_arg.strip().lower()
    if q in ("all", "*"):
        return all_quarters

    raw = quarters_arg.replace(",", " ").split()
    quarters = []
    for item in raw:
        item_u = item.upper().strip()
        if item_u not in all_quarters:
            raise ValueError(f"Invalid quarter: {item}. Use QTR1,QTR2,QTR3,QTR4 or all.")
        quarters.append(item_u)

    seen = set()
    out = []
    for x in quarters:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def parse_years(years_arg, start_year, end_year):
    if years_arg:
        raw = years_arg.replace(",", " ").split()
        years = []
        for item in raw:
            years.append(int(item))
        seen = set()
        out = []
        for y in years:
            if y not in seen:
                seen.add(y)
                out.append(y)
        return out

    if start_year is None or end_year is None:
        raise ValueError("Provide either --years OR both --start-year and --end-year.")

    if start_year > end_year:
        raise ValueError("--start-year must be <= --end-year")

    return list(range(start_year, end_year + 1))


def main():
    parser = argparse.ArgumentParser(description="Download SEC EDGAR company.idx files and store locally/GCS/S3.")

    parser.add_argument("--years", type=str, default="", help="Explicit year list, e.g. '2019,2021,2024' (overrides start/end).")
    parser.add_argument("--start-year", type=int, default=None, help="Start year (YYYY), inclusive.")
    parser.add_argument("--end-year", type=int, default=None, help="End year (YYYY), inclusive.")

    parser.add_argument(
        "--quarters",
        type=str,
        default="all",
        help="Which quarters to download: 'all' or 'QTR1,QTR3' or 'QTR2 QTR4'. Default: all.",
    )

    parser.add_argument("--user-agent", type=str, default=config.DEFAULT_USER_AGENT)
    parser.add_argument("--sleep", type=float, default=config.DEFAULT_SLEEP_SECONDS)

    parser.add_argument("--storage", choices=["local", "gcs", "s3"], required=True)

    # Local
    parser.add_argument("--out-dir", type=str, default=config.DEFAULT_LOCAL_OUT_DIR)

    # GCS
    parser.add_argument("--gcs-bucket", type=str, default=config.DEFAULT_GCS_BUCKET)
    parser.add_argument("--gcs-prefix", type=str, default=config.DEFAULT_GCS_PREFIX)

    # S3
    parser.add_argument("--s3-bucket", type=str, default=config.DEFAULT_S3_BUCKET)
    parser.add_argument("--s3-prefix", type=str, default=config.DEFAULT_S3_PREFIX)
    parser.add_argument("--s3-region", type=str, default=config.DEFAULT_S3_REGION)

    parser.add_argument("--keep-temp", action="store_true")

    args = parser.parse_args()

    years = parse_years(args.years, args.start_year, args.end_year)
    quarters = parse_quarters(args.quarters)

    # Enforce optional dependencies only if used
    if args.storage == "gcs":
        require_gcs()
    if args.storage == "s3":
        require_s3()

    headers = {"User-Agent": args.user_agent}

    # local_root: where we first download
    if args.storage == "local":
        local_root = Path(args.out_dir).expanduser()
    else:
        local_root = Path("./_tmp_idx_downloads").expanduser()

    local_root.mkdir(parents=True, exist_ok=True)

    # destination existence check
    if args.storage == "local":
        def dest_exists(filename):
            return exists_local(args.out_dir, filename)
    elif args.storage == "gcs":
        def dest_exists(filename):
            return exists_gcs(args.gcs_bucket, args.gcs_prefix, filename)
    else:
        def dest_exists(filename):
            return exists_s3(args.s3_bucket, args.s3_prefix, filename, region=(args.s3_region or None))

    print(f"Years selected: {years}")
    print(f"Quarters selected: {quarters}")
    print(f"Storage: {args.storage}")
    print("-" * 60)

    for year in years:
        for quarter in quarters:
            filename = f"{year}_{quarter}_company.idx"
            file_url = f"{config.BASE_URL}{year}/{quarter}/company.idx"

            # Skip if destination already has it
            try:
                if dest_exists(filename):
                    print(f"[SKIP] Already exists: {filename}")
                    continue
            except Exception as e:
                print(f"[WARN] Existence check failed for {filename}. Will attempt download. Error: {e}")

            local_path = local_root / filename
            print(f"[GET ] {file_url}")

            try:
                download_file_to_local(file_url, local_path, headers)
                print(f"[OK  ] Downloaded -> {local_path}")

                if args.storage == "gcs":
                    upload_to_gcs(local_path, args.gcs_bucket, args.gcs_prefix)
                    print(f"[UP  ] Uploaded to GCS -> gs://{args.gcs_bucket}/{(args.gcs_prefix or '').strip('/')}/{filename}".rstrip("/"))

                elif args.storage == "s3":
                    upload_to_s3(local_path, args.s3_bucket, args.s3_prefix, region=(args.s3_region or None))
                    print(f"[UP  ] Uploaded to S3 -> s3://{args.s3_bucket}/{(args.s3_prefix or '').strip('/')}/{filename}".rstrip("/"))

                if args.storage in ("gcs", "s3") and (not args.keep_temp):
                    try:
                        local_path.unlink(missing_ok=True)
                    except Exception:
                        pass

            except Exception as e:
                print(f"[FAIL] {file_url}. Error: {e}")

            time.sleep(args.sleep)

    print("All requested files have been attempted to download.")


if __name__ == "__main__":
    main()
