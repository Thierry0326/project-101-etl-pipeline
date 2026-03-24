# ============================================
# PROJECT 101 - MYSQL LOAD SCRIPT
# Purpose: Load transformed data into
#          MySQL Silver & Gold layers
# ============================================

import pandas as pd
import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
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
        logging.FileHandler('logs/load_mysql.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================
# CONFIGURATION
# ============================================
MYSQL_HOST     = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT     = os.getenv('MYSQL_PORT', '3307')
MYSQL_USER     = os.getenv('MYSQL_USER', 'project101_user')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
BATCH_SIZE     = 1000


# ============================================
# CONNECTION
# ============================================

def get_mysql_connection(database: str):
    """
    Create MySQL connection for specified database.
    Called separately for Silver and Gold layers.
    """
    logger.info(f"🔌 Connecting to MySQL → {database}...")

    try:
        connection_string = (
            f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}"
            f"@{MYSQL_HOST}:{MYSQL_PORT}/{database}"
        )

        engine = create_engine(
            connection_string,
            echo=False,
            pool_pre_ping=True,
            pool_size=5
        )

        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        logger.info(f"✅ Connected to MySQL → {database}!")
        return engine

    except Exception as e:
        logger.error(f"❌ MySQL connection failed: {str(e)}")
        logger.error("💡 Make sure Docker containers are running!")
        raise


# ============================================
# LOAD HELPER
# ============================================

def load_dataframe(
    df: pd.DataFrame,
    table_name: str,
    engine,
    truncate_first: bool = True
) -> int:
    """
    Generic function to load any DataFrame
    into any MySQL table in batches.
    Reusable for both Silver and Gold layers.
    """
    if df.empty:
        logger.warning(f"⚠️ Empty DataFrame for {table_name} - skipping")
        return 0

    total_rows    = len(df)
    loaded_rows   = 0
    total_batches = (total_rows // BATCH_SIZE) + 1

    # Truncate table before loading
    if truncate_first:
        try:
            with engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                conn.commit()
            logger.info(f"🗑️  Truncated {table_name}")
        except Exception as e:
            logger.warning(f"⚠️ Could not truncate {table_name}: {str(e)}")

    logger.info(f"📥 Loading {table_name}...")
    logger.info(f"   Rows: {total_rows:,} | Batches: {total_batches}")

    # Load in batches
    for batch_num, start in enumerate(
        range(0, total_rows, BATCH_SIZE), 1
    ):
        end   = min(start + BATCH_SIZE, total_rows)
        batch = df.iloc[start:end]

        try:
            batch.to_sql(
                name      = table_name,
                con       = engine,
                if_exists = 'append',
                index     = False,
                method    = 'multi'
            )
            loaded_rows += len(batch)

            # Log progress every 10 batches
            if batch_num % 10 == 0 or batch_num == total_batches:
                progress = (loaded_rows / total_rows * 100)
                logger.info(
                    f"   Batch {batch_num}/{total_batches} | "
                    f"{loaded_rows:,}/{total_rows:,} | "
                    f"{progress:.1f}%"
                )

        except Exception as e:
            logger.warning(f"⚠️ Batch {batch_num} failed: {str(e)}")
            continue

    logger.info(f"✅ {table_name} loaded: {loaded_rows:,} rows")
    return loaded_rows


# ============================================
# SILVER LAYER LOAD
# ============================================

def load_silver_layer(transformed_data: dict) -> dict:
    """
    Load all Silver layer tables into
    stackoverflow_processed MySQL database.
    """
    logger.info("\n" + "=" * 50)
    logger.info("🥈 LOADING SILVER LAYER")
    logger.info("=" * 50)

    engine = get_mysql_connection('stackoverflow_processed')
    results = {}

    # Load order matters because of foreign keys!
    # respondents must load FIRST
    load_order = [
        ('respondents',   'respondents'),
        ('education',     'respondent_education'),
        ('compensation',  'respondent_compensation'),
        ('technologies',  'respondent_technologies'),
        ('dev_types',     'respondent_dev_types'),
    ]

    for data_key, table_name in load_order:
        if data_key in transformed_data:
            rows = load_dataframe(
                transformed_data[data_key],
                table_name,
                engine
            )
            results[table_name] = rows
        else:
            logger.warning(f"⚠️ {data_key} not found in transformed data")

    logger.info("✅ Silver layer load complete!")
    return results


# ============================================
# GOLD LAYER LOAD
# ============================================

def load_gold_layer(transformed_data: dict) -> dict:
    """
    Load all Gold layer tables into
    stackoverflow_analytics MySQL database.
    """
    logger.info("\n" + "=" * 50)
    logger.info("🥇 LOADING GOLD LAYER")
    logger.info("=" * 50)

    engine = get_mysql_connection('stackoverflow_analytics')
    results = {}

    # Load dimensions first, then fact table
    load_order = [
        ('dim_developer',  'dim_developer'),
        ('dim_geography',  'dim_geography'),
        ('fact_responses', 'fact_survey_responses'),
    ]

    for data_key, table_name in load_order:
        if data_key in transformed_data:
            rows = load_dataframe(
                transformed_data[data_key],
                table_name,
                engine
            )
            results[table_name] = rows
        else:
            logger.warning(f"⚠️ {data_key} not found in transformed data")

    logger.info("✅ Gold layer load complete!")
    return results


# ============================================
# VERIFICATION
# ============================================

def verify_mysql_loads(silver_results: dict, gold_results: dict):
    """
    Verify all tables loaded correctly.
    Logs row counts for every table.
    """
    logger.info("\n🔍 Verifying MySQL loads...")

    all_results = {**silver_results, **gold_results}
    all_passed  = True

    for table, rows in all_results.items():
        if rows > 0:
            logger.info(f"   ✅ {table:35} → {rows:>8,} rows")
        else:
            logger.warning(f"   ⚠️ {table:35} → {rows:>8,} rows (EMPTY!)")
            all_passed = False

    if all_passed:
        logger.info("✅ All tables verified successfully!")
    else:
        logger.warning("⚠️ Some tables are empty - check logs!")

    return all_passed


def save_mysql_load_report(
    silver_results: dict,
    gold_results: dict
):
    """Save MySQL load summary as audit trail."""
    report = {
        'load_timestamp' : datetime.now().isoformat(),
        'silver_layer'   : silver_results,
        'gold_layer'     : gold_results,
        'total_rows_loaded': sum(silver_results.values()) +
                             sum(gold_results.values())
    }

    os.makedirs('data/processed', exist_ok=True)
    report_path = 'data/processed/mysql_load_report.json'

    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)

    logger.info(f"📝 MySQL load report saved: {report_path}")


# ============================================
# MAIN EXECUTION
# ============================================

def run_mysql_load(transformed_data: dict) -> bool:
    """
    Master MySQL load function.
    Called by Airflow DAG or directly.
    Receives transformed data from transform.py
    """
    logger.info("=" * 50)
    logger.info("PROJECT 101 - MYSQL LOAD PHASE STARTING")
    logger.info("=" * 50)

    # Step 1: Load Silver layer
    silver_results = load_silver_layer(transformed_data)

    # Step 2: Load Gold layer
    gold_results = load_gold_layer(transformed_data)

    # Step 3: Verify everything loaded
    verify_mysql_loads(silver_results, gold_results)

    # Step 4: Save audit report
    save_mysql_load_report(silver_results, gold_results)

    # Final summary
    total = sum(silver_results.values()) + sum(gold_results.values())
    logger.info("=" * 50)
    logger.info("✅ MYSQL LOAD PHASE COMPLETE!")
    logger.info(f"📊 Total rows loaded: {total:,}")
    logger.info("=" * 50)

    return True


if __name__ == "__main__":
    # For testing - run full pipeline
    from extract import run_extraction
    from load import run_load
    from transform import run_transform

    print("🚀 Running full pipeline...")
    df = run_extraction()
    run_load(df)
    transformed = run_transform()
    run_mysql_load(transformed)
    print("\n🎉 Full pipeline complete!")

    

### 📖 What This Script Does:
# ```
# Receives transformed dictionary from transform.py
#         ↓
# 🥈 SILVER LAYER:
# Connects to stackoverflow_processed (MySQL)
# ├── Load respondents          (must be first!)
# ├── Load respondent_education
# ├── Load respondent_compensation
# ├── Load respondent_technologies
# └── Load respondent_dev_types
#         ↓
# 🥇 GOLD LAYER:
# Connects to stackoverflow_analytics (MySQL)
# ├── Load dim_developer        (dimensions first!)
# ├── Load dim_geography
# └── Load fact_survey_responses (fact table last!)
#         ↓
# Verify all row counts
#         ↓
# Save audit report
# ```

# ---

# ### 💡 Two Important Concepts Here:

# **1. Load Order Matters!**
# ```
# # Wrong order - will crash with foreign key error!
# Load respondent_education FIRST ❌
# Load respondents SECOND ❌

# # Correct order - parent before child!
# Load respondents FIRST ✅
# Load respondent_education SECOND ✅
# ```

# **2. Dimensions Before Facts!**
# ```
# # Wrong - fact table references dimensions!
# Load fact_survey_responses FIRST ❌

# # Correct - dimensions must exist first!
# Load dim_developer FIRST ✅
# Load dim_geography SECOND ✅
# Load fact_survey_responses LAST ✅