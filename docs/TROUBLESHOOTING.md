# 🔧 Project 101 — Troubleshooting Guide

This document records all issues encountered during setup
and how they were resolved. Use this as a reference if
you encounter similar problems.

---

## 📋 Table of Contents

1. [Docker Not Starting](#1-docker-not-starting)
2. [Numpy Build Error on Windows](#2-numpy-build-error-on-windows)
3. [Airflow SQLAlchemy Encoding Error](#3-airflow-sqlalchemy-encoding-error)
4. [@ Symbol in Password Breaking Connection String](#4--symbol-in-password-breaking-connection-string)
5. [Airflow Access Denied to MySQL](#5-airflow-access-denied-to-mysql)
6. [Airflow Database Not Initialized](#6-airflow-database-not-initialized)
7. [mysql-connector-python Download Timeout](#7-mysql-connector-python-download-timeout)
8. [Fernet Key Invalid Format](#8-fernet-key-invalid-format)
9. [GitHub Actions - Missing Python Packages](#9-github-actions---missing-python-packages)
10. [Pandas Version Incompatible with Python 3.8](#10-pandas-version-incompatible-with-python-38)

---

## 1. Docker Not Starting

### Symptom

```bash
error during connect: open //./pipe/dockerDesktopLinuxEngine:
The system cannot find the file specified.
```

### Cause

Docker Desktop was not running.

### Fix

- Open Docker Desktop from Windows Start menu
- Wait for **"Engine running"** status at bottom left
- Then retry your docker commands

---

## 2. Numpy Build Error on Windows

### Symptom

```
ERROR: Failed to build 'pandas' when installing build dependencies
Unknown compiler(s): [['icl'], ['cl'], ['cc'], ['gcc']]
```

### Cause

`numpy==1.26.4` requires a C compiler to build from source
on Windows, which is not available by default.

### Fix

In `requirements.txt` change:

```
# Before:
numpy==1.26.4

# After:
numpy>=1.26.0
```

This allows pip to download a pre-built binary wheel
instead of compiling from source.

---

## 3. Airflow SQLAlchemy Encoding Error

### Symptom

```
TypeError: Invalid argument(s) 'encoding' sent to create_engine()
using configuration MySQLDialect_mysqlconnector/QueuePool/Engine
```

### Cause

Airflow 2.8.1 runs on Python 3.8 internally and ships with
SQLAlchemy 1.4.x. Installing SQLAlchemy 2.0.x causes a
version conflict.

### Fix

Pin SQLAlchemy to 1.4.50 in `Dockerfile`:

```dockerfile
RUN pip install --no-cache-dir \
    "sqlalchemy==1.4.50" \
    ...
```

---

## 4. @ Symbol in Password Breaking Connection String

### Symptom

```
Unknown MySQL server host 'mysql@mysql' (-2)
```

### Cause

The `@` symbol in passwords like `Pro101@mysql` breaks
URL parsing. The connection string:

```
mysql+pymysql://user:Pro101@mysql@mysql:3306/db
                         ↑
             @ in password confuses the URL parser!
             MySQL reads host as 'mysql@mysql'
```

### Fix

Remove `@` from all database passwords in `.env`:

```bash
# Before:
MYSQL_PASSWORD=Pro101@mysql
MYSQL_ROOT_PASSWORD=Pro101@mysql

# After:
MYSQL_PASSWORD=Pro101Mysql123
MYSQL_ROOT_PASSWORD=Pro101Mysql123
```

Update the password in MySQL Workbench:

```sql
ALTER USER 'root'@'%' IDENTIFIED BY 'Pro101Mysql123';
ALTER USER 'project101_user'@'%' IDENTIFIED BY 'Pro101Mysql123';
FLUSH PRIVILEGES;
```

---

## 5. Airflow Access Denied to MySQL

### Symptom

```
Access denied for user 'project101_user'@'172.20.0.5'
(using password: YES)
```

### Cause

The `project101_user` did not have permissions on the
`airflow_metadata` database.

### Fix

Run in MySQL Workbench:

```sql
CREATE DATABASE IF NOT EXISTS airflow_metadata;
GRANT ALL PRIVILEGES ON airflow_metadata.*
    TO 'project101_user'@'%';
FLUSH PRIVILEGES;
```

---

## 6. Airflow Database Not Initialized

### Symptom

```
ERROR: You need to initialize the database.
Please run `airflow db init`
```

### Cause

Airflow's metadata tables had not been created in MySQL yet.

### Fix

```bash
# Initialize the database
docker exec -it project101_airflow airflow db migrate

# Create admin user
docker exec -it project101_airflow airflow users create \
  --username admin \
  --password admin101 \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@project101.com

# Restart Airflow
docker restart project101_airflow
```

---

## 7. mysql-connector-python Download Timeout

### Symptom

```
pip._vendor.urllib3.exceptions.ReadTimeoutError:
HTTPSConnectionPool(host='files.pythonhosted.org'):
Read timed out.
```

### Cause

`mysql-connector-python==8.3.0` is 21.5MB and timed out
on slow mobile data connections during Docker build.

### Fix

Remove `mysql-connector-python` from `Dockerfile` and use
`PyMySQL` instead — it's only ~90KB:

```dockerfile
# Before:
RUN pip install mysql-connector-python==8.3.0

# After:
RUN pip install PyMySQL
```

Update connection string to use pymysql driver:

```yaml
# Before:
mysql+mysqlconnector://user:pass@host:3306/db

# After:
mysql+pymysql://user:pass@host:3306/db
```

---

## 8. Fernet Key Invalid Format

### Symptom

```
AirflowException: Could not create Fernet object:
Fernet key must be 32 url-safe base64-encoded bytes.
```

### Cause

Airflow requires a properly formatted Fernet key for
encrypting stored credentials. A plain text password
like `Pro101@Airflow@SecretKey2026` is not valid.

### Fix

Generate a proper Fernet key:

```bash
docker exec -it project101_airflow python -c \
  "from cryptography.fernet import Fernet; \
   print(Fernet.generate_key().decode())"
```

Copy the output and update `.env`:

```bash
AIRFLOW_SECRET_KEY=<generated_key_here>
```

Update `docker-compose.yml`:

```yaml
- AIRFLOW__WEBSERVER__SECRET_KEY=${AIRFLOW_SECRET_KEY}
- AIRFLOW__CORE__FERNET_KEY=${AIRFLOW_SECRET_KEY}
```

---

## 9. GitHub Actions - Missing Python Packages

### Symptom

```
ModuleNotFoundError: No module named 'requests'
```

### Cause

The GitHub Actions workflow was not installing all
required packages for the test suite.

### Fix

Update the install step in `.github/workflows/tests.yml`:

```yaml
- name: 📦 Install Dependencies
  run: |
    pip install pandas numpy python-dotenv pytest \
      pytest-cov sqlalchemy requests tqdm
```

Also add folder creation before tests run:

```yaml
- name: 📁 Create Required Folders
  run: |
    mkdir -p logs
    mkdir -p data/raw
    mkdir -p data/processed
```

---

## 10. Pandas Version Incompatible with Python 3.8

### Symptom

```
ERROR: Could not find a version that satisfies
the requirement pandas==2.2.3
```

### Cause

Airflow 2.8.1 runs Python 3.8 internally. Pandas 2.2.3
requires Python >= 3.9.

### Fix

Use pandas 2.0.3 which is the last version supporting
Python 3.8:

```dockerfile
RUN pip install pandas==2.0.3
```

---

## 🗺️ Environment Configuration Reference

### Port Mapping

| Service    | Windows Port | Docker Internal Port |
| ---------- | ------------ | -------------------- |
| SQL Server | 1434         | 1433                 |
| MySQL      | 3307         | 3306                 |
| Airflow    | 8080         | 8080                 |
| Grafana    | 3000         | 3000                 |

### Connection Strings

| Tool            | Connection              |
| --------------- | ----------------------- |
| SSMS            | `localhost,1434`        |
| MySQL Workbench | `localhost:3307`        |
| Airflow UI      | `http://localhost:8080` |
| Grafana UI      | `http://localhost:3000` |

### Common Docker Commands

```bash
# Start all containers
docker compose up -d

# Stop all containers
docker compose down

# Rebuild and start
docker compose up --build -d

# View logs
docker logs project101_airflow --tail 50

# Execute command in container
docker exec -it project101_airflow airflow db migrate

# Check running containers
docker ps

# Free up space
docker system prune -f
```

---

## 💡 Prevention Tips

1. **Never use `@` in database passwords** — it breaks URL parsing
2. **Always pin package versions** in Dockerfile for reproducibility
3. **Use `AIRFLOW__DATABASE__`** prefix not `AIRFLOW__CORE__` for DB config
4. **Generate proper Fernet keys** — never use plain text passwords
5. **Run `docker compose down` before `up`** when changing env variables
6. **Check `.env` file** first whenever you see connection errors

---

_Last updated: March 2026_
_Project: project-101-etl-pipeline_
