# ============================================
# PROJECT 101 - LOAD SCRIPT
# Purpose: Load extracted data into
#          SQL Server (Bronze/Raw Layer)
# ============================================

import pandas as pd
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
# import sqlalchemy as sa
from sqlalchemy import create_engine, text

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
        logging.FileHandler('logs/load.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================
# CONFIGURATION
# ============================================
MSSQL_SERVER   = os.getenv('MSSQL_SERVER', 'localhost,1434')
MSSQL_DATABASE = 'stackoverflow_raw'
MSSQL_USER     = 'sa'
MSSQL_PASSWORD = os.getenv('MSSQL_SA_PASSWORD')
BATCH_SIZE     = 1000  # Insert 1000 rows at a time


# ============================================
# DATABASE CONNECTION
# ============================================

def get_mssql_connection():
    """
    Create and return SQL Server connection.
    Uses credentials from .env file.
    """
    logger.info("🔌 Connecting to SQL Server...")

    try:
        connection_string = (
            f"mssql+pymssql://{MSSQL_USER}:{MSSQL_PASSWORD}"
            f"@{MSSQL_SERVER}/{MSSQL_DATABASE}"
        )

        engine = create_engine(
            connection_string,
            echo=False,           # Set True to see SQL queries in logs
            pool_pre_ping=True,   # Test connection before using
            pool_size=5           # Connection pool size
        )

        # Test the connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        logger.info("✅ SQL Server connection successful!")
        return engine

    except Exception as e:
        logger.error(f"❌ SQL Server connection failed: {str(e)}")
        logger.error("💡 Make sure Docker containers are running!")
        logger.error("💡 Run: docker compose up -d")
        raise


# ============================================
# PRE-LOAD FUNCTIONS
# ============================================

def truncate_staging_table(engine) -> bool:
    """
    Clear the staging table before loading.
    This ensures we always have fresh data.
    Called a FULL RELOAD strategy.
    """
    logger.info("🗑️  Truncating staging table...")

    try:
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE dbo.survey_responses_raw"))
            conn.commit()

        logger.info("✅ Staging table cleared!")
        return True

    except Exception as e:
        logger.error(f"❌ Truncate failed: {str(e)}")
        raise


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare DataFrame for SQL Server insertion.
    Maps CSV column names to our table column names.
    """
    logger.info("🔧 Preparing DataFrame for loading...")

    # Column mapping: CSV name → Database column name
    column_mapping = {
        'Respondent'                : 'respondent_id',
        'MainBranch'                : 'main_branch',
        'Hobbyist'                  : 'hobbyist',
        'Age'                       : 'age',
        'Gender'                    : 'gender',
        'Country'                   : 'country',
        'Ethnicity'                 : 'ethnicity',
        'EdLevel'                   : 'ed_level',
        'UndergradMajor'            : 'undergraduate_major',
        'Employment'                : 'employment',
        'OrgSize'                   : 'org_size',
        'DevType'                   : 'dev_type',
        'YearsCode'                 : 'years_code',
        'YearsCodePro'              : 'years_code_pro',
        'Currency'                  : 'currency',
        'CompTotal'                 : 'comp_total',
        'CompFreq'                  : 'comp_freq',
        'LanguageWorkedWith'        : 'language_worked_with',
        'LanguageDesireNextYear'    : 'language_desired_next_year',
        'DatabaseWorkedWith'        : 'database_worked_with',
        'DatabaseDesireNextYear'    : 'database_desired_next_year',
        'PlatformWorkedWith'        : 'platform_worked_with',
        'PlatformDesireNextYear'    : 'platform_desired_next_year',
        'WebFrameWorkedWith'        : 'web_frame_worked_with',
        'WebFrameDesireNextYear'    : 'web_frame_desired_next_year',
        'JobSat'                    : 'job_sat',
        'CareerSat'                 : 'career_sat',
        'WorkWeekHrs'               : 'work_week_hrs',
        'OpSys'                     : 'op_sys',
    }

    # Keep only columns we have in our schema
    available_columns = {
        k: v for k, v in column_mapping.items()
        if k in df.columns
    }

    df_prepared = df[list(available_columns.keys())].copy()
    df_prepared = df_prepared.rename(columns=available_columns)

    # Add survey year
    df_prepared['survey_year'] = 2020

    # Replace NaN with None (SQL NULL)
    df_prepared = df_prepared.where(pd.notnull(df_prepared), None)

    logger.info(f"✅ DataFrame prepared!")
    logger.info(f"📊 Rows to load: {len(df_prepared):,}")
    logger.info(f"📊 Columns mapped: {len(df_prepared.columns)}")

    return df_prepared


# ============================================
# LOAD FUNCTIONS
# ============================================

def load_to_sqlserver(df: pd.DataFrame, engine) -> bool:
    """
    Load DataFrame into SQL Server in batches.
    Batch loading prevents memory overload
    and allows progress tracking.
    """
    total_rows   = len(df)
    loaded_rows  = 0
    failed_rows  = 0
    total_batches = (total_rows // BATCH_SIZE) + 1

    logger.info(f"🚀 Starting load to SQL Server...")
    logger.info(f"📊 Total rows    : {total_rows:,}")
    logger.info(f"📦 Batch size    : {BATCH_SIZE:,}")
    logger.info(f"📦 Total batches : {total_batches:,}")

    try:
        # Split DataFrame into batches
        for batch_num, start in enumerate(
            range(0, total_rows, BATCH_SIZE), 1
        ):
            end   = min(start + BATCH_SIZE, total_rows)
            batch = df.iloc[start:end]

            try:
                batch.to_sql(
                    name      = 'survey_responses_raw',
                    con       = engine,
                    schema    = 'dbo',
                    if_exists = 'append',   # Always append to existing table
                    index     = False,      # Don't write DataFrame index
                    method    = 'multi'     # Faster multi-row insert
                )

                loaded_rows += len(batch)

                # Log progress every 10 batches
                if batch_num % 10 == 0 or batch_num == total_batches:
                    progress = (loaded_rows / total_rows * 100)
                    logger.info(
                        f"📦 Batch {batch_num}/{total_batches} | "
                        f"Loaded: {loaded_rows:,}/{total_rows:,} | "
                        f"Progress: {progress:.1f}%"
                    )

            except Exception as batch_error:
                failed_rows += len(batch)
                logger.warning(
                    f"⚠️ Batch {batch_num} failed: {str(batch_error)}"
                )
                continue

        # Final summary
        logger.info("=" * 50)
        logger.info(f"✅ Load complete!")
        logger.info(f"📊 Successfully loaded : {loaded_rows:,} rows")
        logger.info(f"📊 Failed rows         : {failed_rows:,}")
        logger.info(f"📊 Success rate        : {(loaded_rows/total_rows*100):.1f}%")
        logger.info("=" * 50)

        return True

    except Exception as e:
        logger.error(f"❌ Load failed: {str(e)}")
        raise


def verify_load(engine, expected_rows: int) -> bool:
    """
    Verify data landed correctly in SQL Server.
    Compares expected vs actual row count.
    """
    logger.info("🔍 Verifying load...")

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT COUNT(*) FROM dbo.survey_responses_raw")
            )
            actual_rows = result.scalar()

        logger.info(f"📊 Expected rows : {expected_rows:,}")
        logger.info(f"📊 Actual rows   : {actual_rows:,}")

        if actual_rows >= expected_rows * 0.99:  # Allow 1% tolerance
            logger.info("✅ Verification passed!")
            return True
        else:
            logger.warning(
                f"⚠️ Row count mismatch! "
                f"Expected {expected_rows:,} got {actual_rows:,}"
            )
            return False

    except Exception as e:
        logger.error(f"❌ Verification failed: {str(e)}")
        raise


def save_load_report(loaded_rows: int, failed_rows: int):
    """Save load summary as audit trail."""
    import json

    report = {
        'load_timestamp'  : datetime.now().isoformat(),
        'target_database' : 'SQL Server - stackoverflow_raw',
        'target_table'    : 'dbo.survey_responses_raw',
        'loaded_rows'     : loaded_rows,
        'failed_rows'     : failed_rows,
        'success_rate'    : f"{(loaded_rows/(loaded_rows+failed_rows)*100):.1f}%"
        if (loaded_rows + failed_rows) > 0 else "0%"
    }

    os.makedirs('data/raw', exist_ok=True)
    report_path = 'data/raw/load_report.json'

    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)

    logger.info(f"📝 Load report saved: {report_path}")


# ============================================
# MAIN EXECUTION
# ============================================

def run_load(df: pd.DataFrame) -> bool:
    """
    Master load function.
    Called by Airflow DAG or directly.
    Receives DataFrame from extract.py
    """
    logger.info("=" * 50)
    logger.info("PROJECT 101 - LOAD PHASE STARTING")
    logger.info("=" * 50)

    # Step 1: Connect to SQL Server
    engine = get_mssql_connection()

    # Step 2: Clear staging table (full reload)
    truncate_staging_table(engine)

    # Step 3: Prepare DataFrame
    df_prepared = prepare_dataframe(df)

    # Step 4: Load to SQL Server in batches
    load_to_sqlserver(df_prepared, engine)

    # Step 5: Verify load
    verify_load(engine, len(df_prepared))

    # Step 6: Save audit report
    save_load_report(
        loaded_rows=len(df_prepared),
        failed_rows=0
    )

    logger.info("=" * 50)
    logger.info("✅ LOAD PHASE COMPLETE!")
    logger.info(f"📊 {len(df_prepared):,} rows loaded to SQL Server!")
    logger.info("=" * 50)

    return True


if __name__ == "__main__":
    # For testing - run extract then load
    from extract import run_extraction

    df = run_extraction()
    run_load(df)
    print("\n🎉 Extract & Load complete!")


### 📖 What This Script Does — Simply Explained:

# Receives DataFrame from extract.py
#         ↓
# Connects to SQL Server (Docker container)
#         ↓
# Clears staging table (fresh start)
#         ↓
# Maps CSV column names → DB column names
#         ↓
# Inserts data in batches of 1000 rows
#         ↓
# Logs progress every 10 batches
#         ↓
# Verifies row count matches
#         ↓
# Saves load audit report