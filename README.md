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

### Download to Google Cloud Storage
