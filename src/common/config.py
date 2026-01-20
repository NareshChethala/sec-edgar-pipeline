"""
code/common/config.py

Central config for storage defaults and downloader defaults.
You can override any of these via command-line arguments.
"""

# SEC EDGAR base URL
BASE_URL = "https://www.sec.gov/Archives/edgar/full-index/"

# Default quarters
DEFAULT_QUARTERS = ["QTR1", "QTR2", "QTR3", "QTR4"]

# Downloader defaults
DEFAULT_SLEEP_SECONDS = 1.0

# IMPORTANT: SEC wants a descriptive User-Agent (name + email is best)
DEFAULT_USER_AGENT = "Naresh Chethala nareshchandra.chethala@gmail.com"

# Default local output folder (only used when --storage local)
DEFAULT_LOCAL_OUT_DIR = "data/sec_financials/idx"

# Default GCS settings (only used when --storage gcs)
DEFAULT_GCS_BUCKET = "sec-financials-edgar"
DEFAULT_GCS_PREFIX = "idx/company"

# Default S3 settings (only used when --storage s3)
DEFAULT_S3_BUCKET = "my-sec-edgar-bucket"
DEFAULT_S3_PREFIX = "idx/company"
DEFAULT_S3_REGION = "us-east-1"
