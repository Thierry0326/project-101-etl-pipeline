# ============================================
# PROJECT 101 - EXTRACT SCRIPT
# Purpose: Download Stack Overflow 2020 survey
#          Save to data/raw/ folder
#          Read and validate for loading
# ============================================

import pandas as pd
import os
import json
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

# ============================================
# LOGGING SETUP
# ============================================
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/extract.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================
# CONFIGURATION
# ============================================
# Stack Overflow 2020 Survey - Direct download URL
SURVEY_URL    = "https://info.stackoverflowsolutions.com/rs/719-EMH-566" \
                "/images/stack-overflow-developer-survey-2020.zip"
RAW_DATA_PATH = "data/raw/survey_results_public.csv"
SCHEMA_PATH   = "data/raw/survey_results_schema.csv"
ZIP_PATH      = "data/raw/survey_2020.zip"


# ============================================
# DOWNLOAD FUNCTIONS
# ============================================

def extract_zip():
    """Extract the downloaded ZIP file."""
    import zipfile
    logger.info("📦 Extracting ZIP file...")

    with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall('data/raw/')

    # Clean up ZIP after extraction
    os.remove(ZIP_PATH)
    logger.info("✅ ZIP extracted and cleaned up!")

def check_for_updates(url: str, local_path: str) -> bool:
    """
    Check if remote file has been updated
    compared to our local copy.
    Returns True if download needed, False if local is current.
    """
    # If no local file exists, definitely download
    if not os.path.exists(local_path):
        logger.info("📥 No local file found - download required")
        return True

    logger.info("🔍 Checking for updates on remote source...")

    try:
        # Check remote file headers WITHOUT downloading
        response = requests.head(url, timeout=30, allow_redirects=True)
        response.raise_for_status()

        # Get remote file details from headers
        remote_size     = int(response.headers.get('content-length', 0))
        remote_modified = response.headers.get('last-modified', '')

        # Get local file details
        local_size     = os.path.getsize(local_path)
        local_modified = datetime.fromtimestamp(
                            os.path.getmtime(local_path)
                         ).strftime('%a, %d %b %Y %H:%M:%S GMT')

        logger.info(f"📡 Remote → Size: {remote_size/1024**2:.2f}MB | Modified: {remote_modified}")
        logger.info(f"💾 Local  → Size: {local_size/1024**2:.2f}MB  | Modified: {local_modified}")

        # Compare size and modified date
        size_changed     = remote_size != local_size and remote_size > 0
        date_changed     = remote_modified != '' and remote_modified != local_modified

        if size_changed or date_changed:
            logger.info("🆕 Update detected! Re-downloading...")
            return True
        else:
            logger.info("✅ Local file is up to date - skipping download")
            return False

    except requests.exceptions.ConnectionError:
        logger.warning("⚠️ No internet - using local file")
        return False

    except Exception as e:
        logger.warning(f"⚠️ Could not check for updates: {str(e)} - using local file")
        return False


def download_survey_data() -> bool:
    """
    Smart download function.
    Checks for updates before downloading.
    Only re-downloads if remote file has changed.
    """
    logger.info("🔎 Checking data source...")

    # Check if update needed
    needs_download = check_for_updates(SURVEY_URL, RAW_DATA_PATH)

    if not needs_download:
        return True

    logger.info("⬇️  Downloading Stack Overflow 2020 Survey...")
    logger.info(f"🌐 Source: {SURVEY_URL}")

    try:
        response = requests.get(SURVEY_URL, stream=True, timeout=60)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        os.makedirs('data/raw', exist_ok=True)

        with open(ZIP_PATH, 'wb') as f, tqdm(
            desc="Downloading",
            total=total_size,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
        ) as progress:
            for chunk in response.iter_content(chunk_size=8192):
                size = f.write(chunk)
                progress.update(size)

        logger.info("✅ Download complete!")
        extract_zip()
        return True

    except requests.exceptions.ConnectionError:
        # No internet but local file exists - use it!
        if os.path.exists(RAW_DATA_PATH):
            logger.warning("⚠️ No internet but local file exists - using local copy")
            return True

        logger.error("❌ No internet and no local file found!")
        logger.error("📥 Manual download: https://insights.stackoverflow.com/survey")
        logger.error("📁 Place CSV files in: data/raw/")
        return False

    except Exception as e:
        logger.error(f"❌ Download failed: {str(e)}")
        return False


# ============================================
# EXTRACT FUNCTIONS
# ============================================

def check_file_exists(filepath: str) -> bool:
    """Check if the data file exists before processing."""
    if os.path.exists(filepath):
        logger.info(f"✅ File found: {filepath}")
        return True
    else:
        logger.error(f"❌ File not found: {filepath}")
        return False


def get_file_info(filepath: str) -> dict:
    """Get basic info about the CSV file."""
    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    logger.info(f"📁 File size: {size_mb:.2f} MB")
    return {'path': filepath, 'size_mb': round(size_mb, 2)}


def extract_survey_data(filepath: str) -> pd.DataFrame:
    """
    Read raw survey CSV into a pandas DataFrame.
    Everything stored as string - no type conversion yet.
    Type conversion happens in transform phase.
    """
    logger.info("📖 Reading survey data into DataFrame...")

    try:
        df = pd.read_csv(
            filepath,
            dtype=str,            # Raw bronze layer - no type conversion
            keep_default_na=True,
            low_memory=False
        )

        logger.info("✅ Data loaded into memory!")
        logger.info(f"📊 Rows:    {len(df):,}")
        logger.info(f"📊 Columns: {len(df.columns)}")
        logger.info(f"📊 Memory:  {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

        return df

    except Exception as e:
        logger.error(f"❌ Failed to read CSV: {str(e)}")
        raise


def extract_schema_data(filepath: str) -> pd.DataFrame:
    """Read the survey schema/data dictionary."""
    logger.info("📖 Reading survey schema...")

    try:
        schema_df = pd.read_csv(filepath, dtype=str)
        logger.info(f"✅ Schema loaded: {len(schema_df)} column definitions")
        return schema_df

    except Exception as e:
        logger.warning(f"⚠️ Schema file not found: {str(e)}")
        return pd.DataFrame()


def validate_extracted_data(df: pd.DataFrame) -> bool:
    """
    Validate extracted data before loading.
    Acts as first line of defence against bad data.
    """
    logger.info("🔍 Validating extracted data...")

    # Check rows exist
    if len(df) == 0:
        logger.error("❌ No rows found!")
        return False

    # Check columns exist
    if len(df.columns) == 0:
        logger.error("❌ No columns found!")
        return False

    # Check key columns exist
    expected_columns = ['Respondent', 'MainBranch', 'Hobbyist', 'Country']
    missing = [c for c in expected_columns if c not in df.columns]
    if missing:
        logger.error(f"❌ Missing key columns: {missing}")
        return False

    # Warn about high null columns
    null_pct = (df.isnull().sum() / len(df) * 100).round(2)
    high_nulls = null_pct[null_pct > 50]
    if len(high_nulls) > 0:
        logger.warning(f"⚠️ High null columns (>50%): {list(high_nulls.index)}")

    logger.info("✅ Validation passed!")
    return True


def save_extraction_report(df: pd.DataFrame, file_info: dict):
    """Save extraction summary as audit trail."""
    report = {
        'extraction_timestamp': datetime.now().isoformat(),
        'source_file': file_info['path'],
        'file_size_mb': file_info['size_mb'],
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'columns': list(df.columns),
        'null_summary': df.isnull().sum().to_dict()
    }

    os.makedirs('data/raw', exist_ok=True)
    report_path = 'data/raw/extraction_report.json'

    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)

    logger.info(f"📝 Extraction report saved: {report_path}")
    return report


# ============================================
# MAIN EXECUTION
# ============================================
def run_extraction() -> pd.DataFrame:
    """
    Master extraction function.
    Called by Airflow DAG or directly.
    """
    logger.info("=" * 50)
    logger.info("PROJECT 101 - EXTRACT PHASE STARTING")
    logger.info("=" * 50)

    # Step 1: Download if not already present
    if not download_survey_data():
        raise ConnectionError(
            "Could not download survey data. "
            "Please check internet connection or download manually."
        )

    # Step 2: Verify file exists
    if not check_file_exists(RAW_DATA_PATH):
        raise FileNotFoundError(f"CSV not found at {RAW_DATA_PATH}")

    # Step 3: Get file info
    file_info = get_file_info(RAW_DATA_PATH)

    # Step 4: Extract data into DataFrame
    df = extract_survey_data(RAW_DATA_PATH)

    # Step 5: Extract schema
    extract_schema_data(SCHEMA_PATH)

    # Step 6: Validate
    if not validate_extracted_data(df):
        raise ValueError("Data validation failed!")

    # Step 7: Save audit report
    save_extraction_report(df, file_info)

    logger.info("=" * 50)
    logger.info("✅ EXTRACT PHASE COMPLETE!")
    logger.info(f"📊 {len(df):,} rows ready for loading!")
    logger.info("=" * 50)

    return df


if __name__ == "__main__":
    df = run_extraction()
    print(f"\n🎉 Success! {len(df):,} rows extracted.")
    print(f"📋 First 5 columns: {list(df.columns[:5])}")