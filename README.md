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
- `pandas`
- `pyarrow`

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
uv run python -m src.parse_idx \
  --input /path/to/local/idx_files \
  --output /path/to/output/company_index.parquet \
  --format parquet
```

Parse IDX files from GCS
```bash
uv run python -m src.parse_idx \
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

Here is a brief, minimal README for filter_10k_stream.py, written in the same style and level as the previous two scripts, and clearly positioned as the next step in the pipeline.

⸻

# filter_10k_stream.py

## What this script does
- Streams a large SEC filings dataset (CSV or Parquet) without loading it fully into memory.
- Filters the data to keep only **10-K** and **10-K/A** filings.
- Cleans and normalizes filing dates and adds a `Year` field.
- Writes the filtered output in multiple Parquet part files, either locally or to Google Cloud Storage (GCS).

This script is typically used **after parsing `company.idx` files** and before large-scale HTML fetching or modeling.

---
### Filter 10-K filings from a Parquet file and write locally
```bash
python code/filter_10k_stream.py \
  --input /path/to/company_index.parquet \
  --input-format parquet \
  --storage local \
  --out-base data/filtered_10k
```
Filter 10-K filings from Parquet in GCS and write back to GCS
```bash
python code/filter_10k_stream.py \
  --input gs://your-bucket/idx_parsed/company_index.parquet \
  --input-format parquet \
  --storage gcs \
  --out-base edgar_10k_filtered/parts \
  --gcs-bucket your-bucket
```
Filter from CSV input (streamed in chunks)
```bash
python code/filter_10k_stream.py \
  --input /path/to/company_index.csv \
  --input-format csv \
  --storage local \
  --out-base data/filtered_10k
```
⸻

Notes
* The script processes data in batches (CSV chunks or Parquet row groups).
* Output is always written as multiple Parquet part files.
* Designed for large datasets and long-running jobs.
* Supports a --max-batches option for testing on small samples.
---

## One thing to be aware of (not missing, just implicit)

- The script assumes the input dataset already contains valid `Form Type` and `Date Filed` columns (as produced by the previous parsing step).

If you want next, I can:
- Write a **top-level pipeline README** tying all three scripts together
- Add **very short inline comments** for maintainability
- Help you create a **Makefile or runbook** for end-to-end execution

