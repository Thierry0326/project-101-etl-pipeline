# ============================================
# PROJECT 101 - TRANSFORM SCRIPT
# Purpose: Extract from SQL Server (Bronze)
#          Clean & transform data
#          Prepare for MySQL (Silver & Gold)
# ============================================

import pandas as pd
# import numpy as np
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
        logging.FileHandler('logs/transform.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================
# CONFIGURATION
# ============================================
MSSQL_SERVER = os.getenv('MSSQL_SERVER', 'project101_sqlserver,1433')
MSSQL_PASSWORD = os.getenv('MSSQL_SA_PASSWORD')
MSSQL_DATABASE = 'stackoverflow_raw'


# ============================================
# CONNECTION
# ============================================

def get_mssql_connection():
    """Connect to SQL Server to extract raw data."""
    logger.info("🔌 Connecting to SQL Server for extraction...")

    try:
        connection_string = (
            f"mssql+pymssql://sa:{MSSQL_PASSWORD}"
            f"@{MSSQL_SERVER}/{MSSQL_DATABASE}"
        )
        engine = create_engine(connection_string, pool_pre_ping=True)

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        logger.info("✅ Connected to SQL Server!")
        return engine

    except Exception as e:
        logger.error(f"❌ Connection failed: {str(e)}")
        raise


# ============================================
# EXTRACT FROM SQL SERVER
# ============================================

def extract_from_sqlserver(engine) -> pd.DataFrame:
    """
    Extract raw data from SQL Server staging table.
    This is the start of our transform phase.
    """
    logger.info("📤 Extracting raw data from SQL Server...")

    query = "SELECT * FROM dbo.survey_responses_raw"

    try:
        df = pd.read_sql(query, engine)
        logger.info(f"✅ Extracted {len(df):,} rows from SQL Server")
        return df

    except Exception as e:
        logger.error(f"❌ Extraction from SQL Server failed: {str(e)}")
        raise


# ============================================
# TRANSFORM FUNCTIONS
# ============================================

def clean_respondents(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw data into clean respondents table.
    Converts types, standardizes values, handles nulls.
    """
    logger.info("🧹 Cleaning respondents data...")

    cleaned = pd.DataFrame()

    # Respondent ID
    cleaned['respondent_id'] = pd.to_numeric(
        df['respondent_id'], errors='coerce'
    ).astype('Int64')

    # Main branch - standardize
    cleaned['main_branch'] = df['main_branch'].str.strip()

    # Hobbyist - convert Yes/No to Boolean
    cleaned['is_hobbyist'] = df['hobbyist'].map(
        {'Yes': True, 'No': False}
    )

    # Age range - clean text
    cleaned['age_range'] = df['age'].str.strip()

    # Gender - take first value if multiple selected
    cleaned['gender'] = df['gender'].apply(
        lambda x: x.split(';')[0].strip() if pd.notna(x) else None
    )

    # Country - strip whitespace
    cleaned['country'] = df['country'].str.strip()

    # Employment - standardize
    cleaned['employment_status'] = df['employment'].str.strip()

    # Org size
    cleaned['org_size'] = df['org_size'].str.strip()

    # Years coding - handle 'Less than 1 year' and 'More than 50 years'
    cleaned['years_coding'] = df['years_code'].apply(
        clean_years_coding
    )

    # Years coding professionally
    cleaned['years_coding_pro'] = df['years_code_pro'].apply(
        clean_years_coding
    )

    # Work week hours - convert to decimal
    cleaned['work_week_hrs'] = pd.to_numeric(
        df['work_week_hrs'], errors='coerce'
    ).round(2)

    # Job satisfaction
    cleaned['job_satisfaction'] = df['job_sat'].str.strip()

    # Career satisfaction
    cleaned['career_satisfaction'] = df['career_sat'].str.strip()

    # Operating system
    cleaned['operating_system'] = df['op_sys'].str.strip()

    # Survey year
    cleaned['survey_year'] = 2020

    # Drop rows with no respondent ID
    before = len(cleaned)
    cleaned = cleaned.dropna(subset=['respondent_id'])
    dropped = before - len(cleaned)

    if dropped > 0:
        logger.warning(f"⚠️ Dropped {dropped} rows with null respondent_id")

    logger.info(f"✅ Respondents cleaned: {len(cleaned):,} rows")
    return cleaned


def clean_years_coding(value) -> int:
    """
    Convert years coding text to integer.
    Handles special Stack Overflow values.
    """
    if pd.isna(value):
        return None

    value = str(value).strip()

    # Handle special cases
    if 'Less than 1' in value:
        return 0
    if 'More than 50' in value:
        return 51

    # Try converting to number
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def clean_education(df: pd.DataFrame) -> pd.DataFrame:
    """Transform education data into clean table."""
    logger.info("🎓 Cleaning education data...")

    education = pd.DataFrame()
    education['respondent_id'] = pd.to_numeric(
        df['respondent_id'], errors='coerce'
    ).astype('Int64')
    education['education_level']     = df['ed_level'].str.strip()
    education['undergraduate_major'] = df['undergraduate_major'].str.strip()

    education = education.dropna(subset=['respondent_id'])
    logger.info(f"✅ Education cleaned: {len(education):,} rows")
    return education


def clean_compensation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform compensation data.
    Converts salary to numeric and standardizes currency.
    """
    logger.info("💰 Cleaning compensation data...")

    comp = pd.DataFrame()
    comp['respondent_id'] = pd.to_numeric(
        df['respondent_id'], errors='coerce'
    ).astype('Int64')
    comp['currency']       = df['currency'].str.strip()
    comp['comp_total']     = pd.to_numeric(
        df['comp_total'], errors='coerce'
    ).round(2)
    comp['comp_frequency'] = df['comp_freq'].str.strip()

    # Remove unrealistic salaries
    # (less than $100 or more than $10 million)
    before = len(comp)
    comp = comp[
        (comp['comp_total'].isna()) |
        (comp['comp_total'].between(100, 10_000_000))
    ]
    removed = before - len(comp)

    if removed > 0:
        logger.warning(f"⚠️ Removed {removed} unrealistic salary values")

    comp = comp.dropna(subset=['respondent_id'])
    logger.info(f"✅ Compensation cleaned: {len(comp):,} rows")
    return comp


def clean_technologies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform technology data.
    Splits semicolon-separated values into individual rows.
    One row per technology per respondent.
    """
    logger.info("💻 Cleaning technology data...")

    tech_columns = {
        'language_worked_with'       : ('language', True,  False),
        'language_desired_next_year' : ('language', False, True),
        'database_worked_with'       : ('database', True,  False),
        'database_desired_next_year' : ('database', False, True),
        'platform_worked_with'       : ('platform', True,  False),
        'platform_desired_next_year' : ('platform', False, True),
    }

    all_techs = []

    for col, (tech_type, is_current, is_desired) in tech_columns.items():
        if col not in df.columns:
            continue

        # Expand semicolon separated values
        tech_df = df[['respondent_id', col]].copy()
        tech_df = tech_df.dropna(subset=[col])

        # Split each row by semicolon
        tech_df['tech_name'] = tech_df[col].str.split(';')
        tech_df = tech_df.explode('tech_name')
        tech_df['tech_name'] = tech_df['tech_name'].str.strip()
        tech_df = tech_df[tech_df['tech_name'] != '']

        tech_df['tech_type']          = tech_type
        tech_df['is_currently_using'] = is_current
        tech_df['is_desired_next_yr'] = is_desired

        tech_df = tech_df[[
            'respondent_id', 'tech_type',
            'tech_name', 'is_currently_using',
            'is_desired_next_yr'
        ]]

        all_techs.append(tech_df)

    technologies = pd.concat(all_techs, ignore_index=True)
    technologies['respondent_id'] = pd.to_numeric(
        technologies['respondent_id'], errors='coerce'
    ).astype('Int64')

    logger.info(f"✅ Technologies cleaned: {len(technologies):,} rows")
    return technologies


def clean_dev_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform developer type data.
    Splits semicolon-separated values into individual rows.
    """
    logger.info("👨‍💻 Cleaning developer types...")

    dev_df = df[['respondent_id', 'dev_type']].copy()
    dev_df = dev_df.dropna(subset=['dev_type'])

    # Split by semicolon
    dev_df['dev_type'] = dev_df['dev_type'].str.split(';')
    dev_df = dev_df.explode('dev_type')
    dev_df['dev_type'] = dev_df['dev_type'].str.strip()
    dev_df = dev_df[dev_df['dev_type'] != '']

    dev_df['respondent_id'] = pd.to_numeric(
        dev_df['respondent_id'], errors='coerce'
    ).astype('Int64')

    dev_df = dev_df.dropna(subset=['respondent_id'])

    logger.info(f"✅ Dev types cleaned: {len(dev_df):,} rows")
    return dev_df


# ============================================
# GOLD LAYER TRANSFORMS (Star Schema)
# ============================================

def build_dim_developer(cleaned_respondents: pd.DataFrame) -> pd.DataFrame:
    """Build developer dimension table for Gold layer."""
    logger.info("⭐ Building dim_developer...")

    dim = cleaned_respondents[[
        'respondent_id', 'main_branch', 'employment_status',
        'org_size', 'operating_system', 'age_range', 'gender'
    ]].copy()

    dim = dim.drop_duplicates(subset=['respondent_id'])
    logger.info(f"✅ dim_developer: {len(dim):,} rows")
    return dim


def build_dim_geography(cleaned_respondents: pd.DataFrame) -> pd.DataFrame:
    """Build geography dimension table for Gold layer."""
    logger.info("🌍 Building dim_geography...")

    # Country to continent mapping (simplified)
    continent_map = {
        'United States'          : ('North America', 'North America'),
        'United Kingdom'         : ('Europe', 'Western Europe'),
        'Germany'                : ('Europe', 'Western Europe'),
        'Canada'                 : ('North America', 'North America'),
        'India'                  : ('Asia', 'South Asia'),
        'France'                 : ('Europe', 'Western Europe'),
        'Brazil'                 : ('South America', 'South America'),
        'Australia'              : ('Oceania', 'Oceania'),
        'Nigeria'                : ('Africa', 'West Africa'),
        'South Africa'           : ('Africa', 'Southern Africa'),
        'Cameroon'               : ('Africa', 'Central Africa'),
    }

    dim = cleaned_respondents[['country']].drop_duplicates().copy()
    dim['continent'] = dim['country'].map(
        lambda x: continent_map.get(x, ('Other', 'Other'))[0]
    )
    dim['region'] = dim['country'].map(
        lambda x: continent_map.get(x, ('Other', 'Other'))[1]
    )

    logger.info(f"✅ dim_geography: {len(dim):,} rows")
    return dim


def build_fact_table(
    cleaned_respondents: pd.DataFrame,
    cleaned_compensation: pd.DataFrame
) -> pd.DataFrame:
    """Build the main fact table for Gold layer."""
    logger.info("📊 Building fact_survey_responses...")

    fact = cleaned_respondents[[
        'respondent_id', 'years_coding', 'years_coding_pro',
        'work_week_hrs', 'is_hobbyist', 'survey_year',
        'job_satisfaction', 'career_satisfaction'
    ]].copy()

    # Map satisfaction to numeric score
    satisfaction_map = {
        'Very satisfied'      : 5,
        'Slightly satisfied'  : 4,
        'Neither satisfied nor dissatisfied': 3,
        'Slightly dissatisfied': 2,
        'Very dissatisfied'   : 1,
    }

    fact['job_satisfaction_score'] = fact['job_satisfaction'].map(
        satisfaction_map
    )
    fact['career_satisfaction_score'] = fact['career_satisfaction'].map(
        satisfaction_map
    )

    # Join compensation
    fact = fact.merge(
        cleaned_compensation[['respondent_id', 'comp_total']],
        on='respondent_id',
        how='left'
    )
    fact = fact.rename(columns={'comp_total': 'comp_total_usd'})

    logger.info(f"✅ fact_survey_responses: {len(fact):,} rows")
    return fact


def save_transform_report(transformed_data: dict):
    """Save transformation summary as audit trail."""
    report = {
        'transform_timestamp': datetime.now().isoformat(),
        'tables_created': {
            name: len(df)
            for name, df in transformed_data.items()
        }
    }

    report_path = 'data/processed/transform_report.json'
    os.makedirs('data/processed', exist_ok=True)

    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)

    logger.info(f"📝 Transform report saved: {report_path}")


# ============================================
# MAIN EXECUTION
# ============================================

def run_transform() -> dict:
    """
    Master transform function.
    Called by Airflow DAG or directly.
    Returns dictionary of all transformed DataFrames.
    """
    logger.info("=" * 50)
    logger.info("PROJECT 101 - TRANSFORM PHASE STARTING")
    logger.info("=" * 50)

    # Step 1: Connect and extract from SQL Server
    engine = get_mssql_connection()
    raw_df = extract_from_sqlserver(engine)

    # Step 2: Silver layer transforms
    logger.info("\n🥈 SILVER LAYER TRANSFORMS")
    logger.info("-" * 30)
    cleaned_respondents  = clean_respondents(raw_df)
    cleaned_education    = clean_education(raw_df)
    cleaned_compensation = clean_compensation(raw_df)
    cleaned_technologies = clean_technologies(raw_df)
    cleaned_dev_types    = clean_dev_types(raw_df)

    # Step 3: Gold layer transforms
    logger.info("\n🥇 GOLD LAYER TRANSFORMS")
    logger.info("-" * 30)
    dim_developer  = build_dim_developer(cleaned_respondents)
    dim_geography  = build_dim_geography(cleaned_respondents)
    fact_responses = build_fact_table(
        cleaned_respondents,
        cleaned_compensation
    )

    # Step 4: Package all transformed data
    transformed_data = {
        # Silver layer
        'respondents'    : cleaned_respondents,
        'education'      : cleaned_education,
        'compensation'   : cleaned_compensation,
        'technologies'   : cleaned_technologies,
        'dev_types'      : cleaned_dev_types,
        # Gold layer
        'dim_developer'  : dim_developer,
        'dim_geography'  : dim_geography,
        'fact_responses' : fact_responses,
    }

    # Step 5: Save audit report
    save_transform_report(transformed_data)

    # Step 6: Log summary
    logger.info("\n" + "=" * 50)
    logger.info("✅ TRANSFORM PHASE COMPLETE!")
    logger.info("-" * 30)
    for name, df in transformed_data.items():
        logger.info(f"📊 {name:20} → {len(df):>8,} rows")
    logger.info("=" * 50)

    return transformed_data


if __name__ == "__main__":
    transformed = run_transform()
    print("\n🎉 Transform complete!")
    for name, df in transformed.items():
        print(f"   📊 {name}: {len(df):,} rows")


### 📖 What This Script Does:
# ```
# Extract raw data from SQL Server
#         ↓
# 🥈 SILVER TRANSFORMS:
# ├── clean_respondents()    → Fix types, standardize values
# ├── clean_education()      → Clean education levels
# ├── clean_compensation()   → Remove unrealistic salaries
# ├── clean_technologies()   → Split "Python;JavaScript;SQL"
# │                            into separate rows
# └── clean_dev_types()      → Split "Backend;Frontend"
#                              into separate rows
#         ↓
# 🥇 GOLD TRANSFORMS:
# ├── build_dim_developer()  → Developer dimension
# ├── build_dim_geography()  → Geography dimension
# └── build_fact_table()     → Main metrics fact table
#         ↓
# Returns dictionary of all clean DataFrames
# → Passed to load_mysql.py next!
# ```

# ---

# ### 💡 Most Important Transformation:

# The technology splitting is very clever — Stack Overflow stores data like:
# ```
# # Raw (Bronze - SQL Server):
# language_worked_with = "Python;JavaScript;SQL;HTML"

# # Clean (Silver - MySQL):
# respondent_id | tech_type  | tech_name   | is_currently_using
# 1             | language   | Python      | True
# 1             | language   | JavaScript  | True
# 1             | language   | SQL         | True
# 1             | language   | HTML        | True