# project-101-etl-pipeline

Local ETL Pipeline using Python, SQL Server, MySQL, Airflow, Grafana and Power BI

# 🚀 Project 101 — Local ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.13-blue?logo=python)
![SQL Server](https://img.shields.io/badge/SQL%20Server-2022-red?logo=microsoftsqlserver)
![MySQL](https://img.shields.io/badge/MySQL-8.0-orange?logo=mysql)
![Apache Airflow](https://img.shields.io/badge/Airflow-2.8.1-green?logo=apacheairflow)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)
![Grafana](https://img.shields.io/badge/Grafana-Monitoring-orange?logo=grafana)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow?logo=powerbi)
![GitHub](https://img.shields.io/badge/GitHub-Version%20Control-black?logo=github)

---

## 📖 Overview

A fully local, production-grade **ELT + ETL hybrid data pipeline** built on the
Stack Overflow 2020 Developer Survey dataset. This project demonstrates a complete
**Medallion Architecture** (Bronze → Silver → Gold) using industry-standard tools.

This is **Project 1** of a two-part series:

- 🏠 **Project 1 (This project):** Full local pipeline
- ☁️ **Project 2 (Coming soon):** Same pipeline migrated to AWS & Azure

---

## 🏗️ Architecture

```
[Stack Overflow 2020 CSV]
          │
          ▼ Python (Auto-download & Extract)
┌─────────────────────────┐
│  SQL Server 2022        │  ← 🥉 BRONZE LAYER (Raw/Staging)
│  stackoverflow_raw      │     Untouched data, all NVARCHAR
└─────────────────────────┘
          │
          ▼ Python (Transform & Clean)
┌─────────────────────────┐
│  MySQL 8.0              │  ← 🥈 SILVER LAYER (Processed)
│  stackoverflow_processed│     Normalized, typed, relational
└─────────────────────────┘
          │
          ▼ Python (Aggregate & Model)
┌─────────────────────────┐
│  MySQL 8.0              │  ← 🥇 GOLD LAYER (Analytics)
│  stackoverflow_analytics│     Star schema, BI-ready
└─────────────────────────┘
          │
          ▼
┌─────────────────────────┐
│  Power BI Dashboard     │  ← 📊 VISUALIZATION
└─────────────────────────┘

All orchestrated by Apache Airflow
All monitored by Grafana
All containerized with Docker
```

---

## 🛠️ Tech Stack

| Layer               | Tool                 | Purpose                          |
| ------------------- | -------------------- | -------------------------------- |
| **Orchestration**   | Apache Airflow 2.8.1 | Pipeline scheduling & automation |
| **Bronze DB**       | SQL Server 2022      | Raw data staging                 |
| **Silver/Gold DB**  | MySQL 8.0            | Processed & analytics data       |
| **Transforms**      | Python 3.13 + Pandas | Data cleaning & modeling         |
| **Data Quality**    | Great Expectations   | Data validation                  |
| **Monitoring**      | Grafana              | Pipeline & DB monitoring         |
| **Visualization**   | Power BI             | Business intelligence dashboards |
| **Containers**      | Docker + Compose     | Service containerization         |
| **Testing**         | Pytest               | Automated test suite             |
| **Version Control** | Git + GitHub         | Source control                   |

---

## 📁 Project Structure

```
project-101-etl-pipeline/
│
├── 📁 pipeline/
│   ├── extract.py          # Auto-download & extract CSV
│   ├── load.py             # Load raw data → SQL Server
│   ├── transform.py        # Clean & transform data
│   └── load_mysql.py       # Load clean data → MySQL
│
├── 📁 dags/
│   └── etl_pipeline.py     # Airflow DAG (full orchestration)
│
├── 📁 sql/
│   ├── mssql_schema.sql    # SQL Server Bronze schema
│   ├── mysql_schema.sql    # MySQL Silver & Gold schemas
│   └── security_setup.sql  # Database users & permissions
│
├── 📁 tests/
│   └── test_pipeline.py    # Pytest test suite (30+ tests)
│
├── 📁 expectations/
│   └── data_quality.py     # Great Expectations rules
│
├── 📁 monitoring/
│   └── grafana_dashboard.json  # Grafana dashboard config
│
├── 📁 data/
│   ├── raw/                # Downloaded CSV files (not in Git)
│   └── processed/          # Transform reports
│
├── 📁 docs/
│   └── architecture.png    # Architecture diagram
│
├── 📁 backups/             # Database backups (not in Git)
├── 📁 logs/                # Pipeline logs (not in Git)
│
├── 🐳 docker-compose.yml   # All containers defined here
├── 📋 requirements.txt     # Python dependencies
├── 🔐 .env.example         # Environment template (safe)
├── 🚫 .gitignore           # Excludes secrets & data files
└── 📖 README.md            # You are here!
```

---

## 🚀 Quick Start

### Prerequisites

Make sure you have these installed:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Python 3.10+](https://www.python.org/downloads/)
- [Git](https://git-scm.com/)
- [Power BI Desktop](https://powerbi.microsoft.com/) (optional)

---

### Step 1 — Clone the Repository

```bash
git clone https://github.com/Thierry0326/project-101-etl-pipeline.git
cd project-101-etl-pipeline
```

### Step 2 — Configure Environment

```bash
# Copy the environment template
cp .env.example .env

# Open .env and set your own passwords
# Never share or commit this file!
```

### Step 3 — Create Virtual Environment

```bash
python -m venv venv
source venv/Scripts/activate  # Windows
# source venv/bin/activate    # Mac/Linux

pip install -r requirements.txt
```

### Step 4 — Start Docker Containers

```bash
docker compose up -d
```

This starts:

- ✅ SQL Server on `localhost,1434`
- ✅ MySQL on `localhost:3307`
- ✅ Airflow on `http://localhost:8080`
- ✅ Grafana on `http://localhost:3000`

### Step 5 — Run Database Schemas

```bash
# Connect to SQL Server (SSMS: localhost,1434)
# Run: sql/mssql_schema.sql

# Connect to MySQL (Workbench: localhost:3307)
# Run: sql/mysql_schema.sql
# Run: sql/security_setup.sql
```

### Step 6 — Run the Pipeline

```bash
# Option A: Run manually
python pipeline/extract.py
python pipeline/load.py
python pipeline/transform.py
python pipeline/load_mysql.py

# Option B: Via Airflow UI
# Go to http://localhost:8080
# Enable DAG: project101_etl_pipeline
# Click: Trigger DAG
```

### Step 7 — Run Tests

```bash
pytest tests/ -v
```

---

## 📊 Pipeline Phases

### 🥉 Phase 1: Extract & Load (Bronze)

- Python auto-downloads Stack Overflow 2020 survey
- Checks for updates before re-downloading
- Validates data quality before loading
- Loads ~65,000 rows into SQL Server as-is

### 🥈 Phase 2: Transform & Load (Silver)

- Extracts raw data from SQL Server
- Cleans data types (NVARCHAR → proper types)
- Normalizes into relational tables
- Splits semicolon-separated values into rows
- Removes unrealistic salary values
- Loads into MySQL normalized tables

### 🥇 Phase 3: Aggregate & Model (Gold)

- Builds Star Schema for analytics
- Creates fact and dimension tables
- Maps satisfaction text to numeric scores
- Optimized for Power BI queries

---

## 🔌 Connecting External Tools

### SSMS → Docker SQL Server

```
Server: localhost,1434
Authentication: SQL Server Authentication
Login: sa
Password: (from your .env file)
```

### MySQL Workbench → Docker MySQL

```
Host: localhost
Port: 3307
Username: project101_user
Password: (from your .env file)
```

### Power BI → MySQL Analytics

```
Connector: MySQL (requires MySQL Connector/NET)
Server: localhost
Port: 3307
Database: stackoverflow_analytics
Username: analytics_user
```

### Grafana → http://localhost:3000

```
Username: admin
Password: (from your .env file)
```

### Airflow → http://localhost:8080

```
Username: admin
Password: (from your .env file)
```

---

## 🔐 Security

- All credentials stored in `.env` file (never committed)
- Dedicated database users with minimum permissions
- ETL user has INSERT/SELECT only
- Analytics user has SELECT only (read-only for Power BI)
- `.env` is in `.gitignore` — safe to clone publicly

---

## 🗺️ Roadmap

- [x] Project structure & GitHub setup
- [x] Docker Compose configuration
- [x] SQL Server Bronze schema
- [x] MySQL Silver & Gold schemas
- [x] Python extract script (with auto-download)
- [x] Python load script (SQL Server)
- [x] Python transform script (Medallion architecture)
- [x] Python load script (MySQL)
- [x] Airflow DAG orchestration
- [x] Pytest test suite
- [ ] Great Expectations data quality
- [ ] Grafana monitoring dashboards
- [ ] Power BI dashboard
- [ ] Project 2: Cloud migration (AWS + Azure)

---

## 👤 Author

**Thierry** — Database Administrator transitioning to Cloud Data Engineer

[![GitHub](https://img.shields.io/badge/GitHub-Thierry0326-black?logo=github)](https://github.com/Thierry0326)

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

> 💡 **Note:** This is Project 1 of 2. Project 2 will migrate this
> entire pipeline to AWS (Glue, RDS, MWAA) and Azure
> (Data Factory, Azure SQL, Managed Airflow).
