# download_company_idx.py

## What this script does
- Downloads SEC EDGAR `company.idx` index files for selected years and quarters.
- Supports storing files locally, in Google Cloud Storage (GCS), or in Amazon S3.
- Skips files that already exist and respects SEC rate limits.

---

## Requirements

### Base
- Python 3.9+
- `requests`

### If using Google Cloud Storage
- `google-cloud-storage`
- Google Application Default Credentials (ADC) configured

### If using Amazon S3
- `boto3`
- AWS credentials configured

---

## How to use

Run from the project root.

### Download to local storage
```bash
python code/download_company_idx.py \
  --storage local \
  --start-year 2020 \
  --end-year 2021
```
### Download to Google Cloud Storage
```bash
python code/download_company_idx.py \
  --storage gcs \
  --start-year 2020 \
  --end-year 2021 \
  --gcs-bucket your-bucket \
  --gcs-prefix your-prefix
```

### Download to Amazon S3
```bash
python code/download_company_idx.py \
  --storage s3 \
  --start-year 2020 \
  --end-year 2021 \
  --s3-bucket your-bucket \
  --s3-prefix your-prefix
```
You must provide either:
* --years
* OR both --start-year and --end-year


Here is a brief, clean README for this second script, written as a continuation in style from the first one. It only covers what it does → requirements → how to use, with no deep internals.

⸻


# parse_idx.py

## What this script does
- Parses SEC EDGAR `company.idx` files into a structured dataset.
- Supports reading `.idx` files from a local directory or a Google Cloud Storage (GCS) prefix.
- Outputs a single consolidated dataset in CSV or Parquet format (Parquet recommended).
- Designed to be used after downloading raw `company.idx` files.

---
### Parse IDX files from local directory
```bash
uv run python -m src.parse_company_idx \
  --input /path/to/local/idx_files \
  --output /path/to/output/company_index.parquet \
  --format parquet
```

Parse IDX files from GCS
```bash
uv run python -m src.parse_company_idx \
  --input gs://your-bucket/idx_files \
  --output gs://your-bucket/idx_parsed/company_index.parquet \
  --format parquet
```
Optional arguments
* --format
Output format: csv or parquet (default: parquet)
* --skip-if-exists
Skip processing if the output file already exists
⸻

Notes
* All .idx files are combined into a single dataset.
* The script is tolerant to malformed rows and skips invalid lines.
* Output is written locally or to GCS based on the output path.
---
- The script assumes all input `.idx` files follow the standard SEC `company.idx` format.  
  Any non-standard rows are skipped safely.


