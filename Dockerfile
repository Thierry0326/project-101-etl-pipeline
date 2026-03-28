# ============================================
# PROJECT 101 - CUSTOM AIRFLOW IMAGE
# Extends official Airflow with our
# Python dependencies pre-installed
# ============================================

FROM apache/airflow:2.8.1

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

# Install Python dependencies inside container
# This means Airflow DAGs can import pandas,
# sqlalchemy, pymssql etc without issues!
RUN pip install --no-cache-dir \
    pandas==2.0.3 \
    numpy \
    sqlalchemy==2.0.25 \
    pymssql==2.2.11 \
    mysql-connector-python==8.3.0 \
    python-dotenv==1.0.0 \
    requests \
    tqdm==4.66.1 \
    loguru==0.7.2


    RUN pip install --no-cache-dir \
    pandas==2.0.3 \
    numpy \
    sqlalchemy==2.0.25 \
    pymssql==2.2.11 \
    mysql-connector-python==8.3.0 \
    PyMySQL \
    python-dotenv==1.0.0 \
    requests \
    tqdm==4.66.1 \
    loguru==0.7.2