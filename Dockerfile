# ============================================
# PROJECT 101 - CUSTOM AIRFLOW IMAGE
# Extends official Airflow with our
# Python dependencies pre-installed
# ============================================

FROM apache/airflow:2.11.1-python3.12

USER root

# Install system dependencies
# Required for pyodbc (SQL Server connection)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    unixodbc-dev \
    curl \
    gnupg2 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements file into container
COPY requirements.txt .

# Install Python dependencies inside container.
# Pins match requirements.txt so DAGs import the same versions
# whether running in Airflow or locally.
RUN pip install --no-cache-dir \
    "sqlalchemy>=1.4.54,<2.0" \
    "pandas>=2.1,<2.2" \
    "numpy>=1.26,<2.3" \
    PyMySQL==1.1.2 \
    pymssql==2.3.13 \
    python-dotenv==1.1.0 \
    requests \
    tqdm==4.67.3 \
    loguru==0.7.3