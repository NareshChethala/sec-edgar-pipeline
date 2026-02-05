import pandas as pd
import gcsfs
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Configuration
BASE_PATH = "gs://sec-financials-edgar/sec_edgar_financials/raw/parquet"
OUTPUT_PATH = "gs://sec-financials-edgar/sec_edgar_financials/processed"
YEARS = [2009, 2010, 2011]
QUARTERS = [1, 2, 3, 4]
FORMS_TO_KEEP = ["10-K", "10-K/A"]


def load_4q_year_gcs(base_path: str, year: int, **context):
    """Load quarterly parquet files from GCS for a given year."""
    fs = gcsfs.GCSFileSystem()
    
    df_num = {}
    df_sub = {}
    df_tag = {}
    
    for q in QUARTERS:
        qname = f"q{q}"
        
        num_path = f"{base_path}/{year}q{q}/num.parquet"
        sub_path = f"{base_path}/{year}q{q}/sub.parquet"
        tag_path = f"{base_path}/{year}q{q}/tag.parquet"
        
        try:
            df_num[qname] = pd.read_parquet(num_path, filesystem=fs) if fs.exists(num_path) else None
            df_sub[qname] = pd.read_parquet(sub_path, filesystem=fs) if fs.exists(sub_path) else None
            df_tag[qname] = pd.read_parquet(tag_path, filesystem=fs) if fs.exists(tag_path) else None
            
            logger.info(f"Loaded Q{q} {year} successfully")
        except Exception as e:
            logger.error(f"Error loading Q{q} {year}: {str(e)}")
            raise
    
    # Push to XCom for downstream tasks
    context['task_instance'].xcom_push(key=f'num_{year}', value=df_num)
    context['task_instance'].xcom_push(key=f'sub_{year}', value=df_sub)
    context['task_instance'].xcom_push(key=f'tag_{year}', value=df_tag)
    
    return f"Loaded data for year {year}"
