# ============================================
# PROJECT 101 - AIRFLOW DAG
# Purpose: Orchestrate the full ETL pipeline
# Schedule: Daily at midnight
# ============================================

import os
from dotenv import load_dotenv
load_dotenv()


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

# ============================================
# DEFAULT ARGUMENTS
# Applied to every task in the DAG
# ============================================
default_args = {
    'owner'             : 'project101',
    'depends_on_past'   : False,
    'start_date'        : days_ago(1),
    'email'             : ['AIRFLOW_ALERT_EMAIL', ''],  # Set in .env
    'email_on_failure'  : True,     # Email if task fails
    'email_on_retry'    : False,
    'retries'           : 2,        # Retry failed tasks twice
    'retry_delay'       : timedelta(minutes=5),  # Wait 5 mins between retries
}


# ============================================
# TASK FUNCTIONS
# Each function wraps our pipeline scripts
# ============================================

def task_extract(**context):
    """
    Task 1: Extract
    Downloads and reads Stack Overflow CSV
    """
    import sys
    sys.path.insert(0, '/opt/airflow/dags/../pipeline')

    from extract import run_extraction

    logger.info("🚀 Starting Extract task...")
    df = run_extraction()

    # Save row count to XCom so next tasks can see it
    context['ti'].xcom_push(key='extracted_rows', value=len(df))
    logger.info(f"✅ Extract complete: {len(df):,} rows")

    return len(df)


def task_load_sqlserver(**context):
    """
    Task 2: Load to SQL Server (Bronze)
    Loads raw data into SQL Server staging table
    """
    import sys
    sys.path.insert(0, '/opt/airflow/dags/../pipeline')

    from extract import run_extraction
    from load import run_load

    logger.info("🚀 Starting Load to SQL Server task...")
    df = run_extraction()
    run_load(df)

    logger.info("✅ Load to SQL Server complete!")


def task_transform(**context):
    """
    Task 3: Transform
    Cleans and transforms data from SQL Server
    Prepares Silver and Gold layer DataFrames
    """
    import sys
    sys.path.insert(0, '/opt/airflow/dags/../pipeline')

    from transform import run_transform

    logger.info("🚀 Starting Transform task...")
    transformed_data = run_transform()

    # Log summary
    for name, df in transformed_data.items():
        logger.info(f"   📊 {name}: {len(df):,} rows")

    logger.info("✅ Transform complete!")


def task_load_mysql(**context):
    """
    Task 4: Load to MySQL (Silver & Gold)
    Loads transformed data into MySQL
    """
    import sys
    sys.path.insert(0, '/opt/airflow/dags/../pipeline')

    from extract import run_extraction
    from load import run_load
    from transform import run_transform
    from load_mysql import run_mysql_load

    logger.info("🚀 Starting Load to MySQL task...")
    df           = run_extraction()
    run_load(df)
    transformed  = run_transform()
    run_mysql_load(transformed)

    logger.info("✅ Load to MySQL complete!")


def task_validate_pipeline(**context):
    """
    Task 5: Validate
    Checks all tables have data after pipeline run
    Sends alert if something is wrong
    """
    import sys
    sys.path.insert(0, '/opt/airflow/dags/../pipeline')

    from sqlalchemy import create_engine, text
    from dotenv import load_dotenv
    import os

    load_dotenv()

    logger.info("🔍 Validating full pipeline...")

    # Check SQL Server
    mssql_engine = create_engine(
        f"mssql+pymssql://sa:{os.getenv('MSSQL_SA_PASSWORD')}"
        f"@{os.getenv('MSSQL_SERVER', 'localhost,1434')}/stackoverflow_raw"
    )

    with mssql_engine.connect() as conn:
        mssql_rows = conn.execute(
            text("SELECT COUNT(*) FROM dbo.survey_responses_raw")
        ).scalar()

    # Check MySQL Silver
    mysql_engine = create_engine(
        f"mysql+mysqlconnector://{os.getenv('MYSQL_USER')}:"
        f"{os.getenv('MYSQL_PASSWORD')}@localhost:3307"
        f"/stackoverflow_processed"
    )

    with mysql_engine.connect() as conn:
        mysql_rows = conn.execute(
            text("SELECT COUNT(*) FROM respondents")
        ).scalar()

    logger.info(f"📊 SQL Server rows : {mssql_rows:,}")
    logger.info(f"📊 MySQL rows      : {mysql_rows:,}")

    # Fail the task if tables are empty
    if mssql_rows == 0:
        raise ValueError("❌ SQL Server table is empty!")

    if mysql_rows == 0:
        raise ValueError("❌ MySQL respondents table is empty!")

    logger.info("✅ Pipeline validation passed!")


def task_notify_success(**context):
    """
    Task 6: Notify
    Logs success message after full pipeline run
    Later connected to Grafana email alerts
    """
    run_date = context['ds']
    logger.info("=" * 50)
    logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info(f"📅 Run date: {run_date}")
    logger.info("=" * 50)


# ============================================
# DAG DEFINITION
# ============================================
with DAG(
    dag_id              = 'project101_etl_pipeline',
    description         = 'Stack Overflow 2020 ETL Pipeline',
    default_args        = default_args,
    schedule_interval   = '0 0 * * *',  # Daily at midnight
    catchup             = False,         # Don't backfill missed runs
    max_active_runs     = 1,             # Only one run at a time
    tags                = ['project101', 'etl', 'stackoverflow'],
) as dag:

    # ==========================================
    # DEFINE TASKS
    # ==========================================

    start = EmptyOperator(
        task_id = 'start_pipeline'
    )

    extract = PythonOperator(
        task_id         = 'extract_data',
        python_callable = task_extract,
    )

    load_sqlserver = PythonOperator(
        task_id         = 'load_to_sqlserver',
        python_callable = task_load_sqlserver,
    )

    transform = PythonOperator(
        task_id         = 'transform_data',
        python_callable = task_transform,
    )

    load_mysql = PythonOperator(
        task_id         = 'load_to_mysql',
        python_callable = task_load_mysql,
    )

    validate = PythonOperator(
        task_id         = 'validate_pipeline',
        python_callable = task_validate_pipeline,
    )

    notify = PythonOperator(
        task_id         = 'notify_success',
        python_callable = task_notify_success,
    )

    end = EmptyOperator(
        task_id = 'end_pipeline'
    )

    # ==========================================
    # TASK DEPENDENCIES
    # This defines the order of execution!
    # ==========================================
    start >> extract >> load_sqlserver >> transform >> load_mysql >> validate >> notify >> end
# ```

# ---

# ### 📖 What This DAG Does — Visually:
# ```
# [start]
#    ↓
# [extract_data]          ← Downloads & reads CSV
#    ↓
# [load_to_sqlserver]     ← Loads raw data to SQL Server
#    ↓
# [transform_data]        ← Cleans & transforms data
#    ↓
# [load_to_mysql]         ← Loads to Silver & Gold MySQL
#    ↓
# [validate_pipeline]     ← Checks all tables have data
#    ↓
# [notify_success]        ← Logs success message
#    ↓
# [end]