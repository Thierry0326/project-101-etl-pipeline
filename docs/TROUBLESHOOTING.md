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
11. [SQLAlchemy 1.4 Connection Has No .commit()](#11-sqlalchemy-14-connection-has-no-commit)
12. [Pandas 2.2 Silently Breaks With SQLAlchemy 1.4](#12-pandas-22-silently-breaks-with-sqlalchemy-14)
13. [18456 Login Failed Masks Missing Database](#13-18456-login-failed-masks-missing-database)
14. [MSSQL SA Password Fails Complexity Policy](#14-mssql-sa-password-fails-complexity-policy)
15. [MSSQL Volume Retains Old SA Password](#15-mssql-volume-retains-old-sa-password)
16. [MySQL TRUNCATE Blocked by Foreign Keys](#16-mysql-truncate-blocked-by-foreign-keys)
17. [NVARCHAR Columns Too Narrow for Multi-Value Data](#17-nvarchar-columns-too-narrow-for-multi-value-data)
18. [Transform DataFrame Has Extra Columns](#18-transform-dataframe-has-extra-columns)
19. [Silent Batch-Failure in Load Tasks](#19-silent-batch-failure-in-load-tasks)
20. [Airflow Scheduler Not Running](#20-airflow-scheduler-not-running)
21. [Truncated Downloads Not Detected](#21-truncated-downloads-not-detected)
22. [Redundant run_extraction in Downstream Tasks](#22-redundant-run_extraction-in-downstream-tasks)
23. [Git Bash Path Conversion Breaks docker exec](#23-git-bash-path-conversion-breaks-docker-exec)

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

## 11. SQLAlchemy 1.4 Connection Has No .commit()

### Symptom

```
AttributeError: 'Connection' object has no attribute 'commit'
```

Raised when calling `conn.commit()` after `with engine.connect() as conn:`.

### Cause

In SQLAlchemy 1.4 *legacy* mode (which Airflow 2.x pins), `engine.connect()`
returns a Connection without a `.commit()` method. `commit()` is only
available when the engine is created with `future=True` (2.0-style).

### Fix

Use `engine.begin()` instead — it opens a transaction and auto-commits
on clean context exit, rolling back on exception. Works in both 1.4 and 2.0:

```python
# Before (fails on SQLA 1.4 legacy):
with engine.connect() as conn:
    conn.execute(text("TRUNCATE TABLE foo"))
    conn.commit()

# After:
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE foo"))
```

---

## 12. Pandas 2.2 Silently Breaks With SQLAlchemy 1.4

### Symptom

```
UserWarning: pandas only supports SQLAlchemy connectable (engine/connection)
or database string URI or sqlite3 DBAPI2 connection...

AttributeError: 'Engine' object has no attribute 'cursor'
```

Every `pd.read_sql(query, engine)` and `df.to_sql(..., con=engine)` fails
with `cursor` missing. Load tasks silently report "success" with 0 rows
if batch errors are caught.

### Cause

Pandas 2.2 hardcodes `"sqlalchemy": "2.0.0"` as its minimum version in
`pandas/compat/_optional.py`. When it detects SQLAlchemy < 2.0, its
internal `import_optional_dependency("sqlalchemy")` returns `None`, so
pandas treats your engine as a raw DBAPI connection and calls
`engine.cursor()` — which doesn't exist on SQLAlchemy Engine/Connection
objects.

Airflow 2.11 hard-pins `sqlalchemy<2.0` via flask-appbuilder<1.5, so you
cannot bump SQLAlchemy to fix this.

### Fix

Downgrade pandas to 2.1.x (last line that accepts SQLAlchemy 1.4):

```txt
# requirements.txt
pandas>=2.1,<2.2
sqlalchemy>=1.4.54,<2.0
```

```dockerfile
# Dockerfile
RUN pip install --no-cache-dir \
    "sqlalchemy>=1.4.54,<2.0" \
    "pandas>=2.1,<2.2" \
    ...
```

Then `docker compose build --no-cache && docker compose up -d`.

---

## 13. 18456 Login Failed Masks Missing Database

### Symptom

```
pymssql.exceptions.OperationalError: (18456, b"Login failed for user 'sa'.
DB-Lib error message 20018, severity 14: General SQL Server error...")
```

Password is definitely correct (verified by `sqlcmd` logging in fine).
Connection string is correct. But SQL Server still rejects the login.

### Cause

SQL Server returns **18456 Login Failed** even when authentication
succeeded but the explicitly-requested database does not exist. The
connection string `mssql+pymssql://sa:pwd@host/stackoverflow_raw`
tries to connect directly to `stackoverflow_raw`; if that DB doesn't
exist, you get 18456 that *looks* like a password error.

### Fix

Read SQL Server's own error log for the real reason:

```bash
MSYS_NO_PATHCONV=1 docker exec project101_sqlserver \
  sh -c "grep -a 'Login failed' /var/opt/mssql/log/errorlog | tail -5"
```

You'll see the actual cause, e.g.:

```
Login failed for user 'sa'. Reason: Failed to open the explicitly
specified database 'stackoverflow_raw'.
```

Then run the schema file to create the missing DB:

```bash
docker cp sql/mssql_schema.sql project101_sqlserver:/tmp/
MSYS_NO_PATHCONV=1 docker exec project101_sqlserver \
  //opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourPwd' -C \
  -i /tmp/mssql_schema.sql
```

---

## 14. MSSQL SA Password Fails Complexity Policy

### Symptom

SQL Server container stuck in a crash loop (`Restarting (255)`). Logs:

```
ERROR: Unable to set system administrator password: Password validation
failed. The password does not meet SQL Server password policy requirements
because it is not complex enough. The password must be at least 8 characters
long and contain characters from three of the following four sets:
Uppercase letters, Lowercase letters, Base 10 digits, and Symbols.
```

### Cause

SQL Server 2022 enforces password complexity on first boot. `pro101mssql123`
has only lowercase + digits = 2 of 4 classes → rejected.

### Fix

Use a password with at least 3 of 4 character classes in `.env`:

```bash
# Before (fails policy):
MSSQL_SA_PASSWORD=pro101mssql123

# After (uppercase + lowercase + digits = 3 classes):
MSSQL_SA_PASSWORD=Pro101Mssql123
```

Then restart the sqlserver service:

```bash
docker compose up -d sqlserver
```

---

## 15. MSSQL Volume Retains Old SA Password

### Symptom

Even after updating `MSSQL_SA_PASSWORD` in `.env` and restarting the
sqlserver container, Airflow still gets:

```
Login failed for user 'sa'. Reason: Password did not match that for the
login provided.
```

### Cause

MSSQL only reads `MSSQL_SA_PASSWORD` on the **first** container boot
(empty volume). On subsequent boots, it uses the password stored in
`master.mdf` and ignores the env var.

### Fix

Either log in with the old password and update in-place:

```bash
docker exec -it project101_sqlserver //opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'OldPassword' -C \
  -Q "ALTER LOGIN sa WITH PASSWORD = 'NewPassword'; PRINT 'done';"
```

Or, if you don't remember the old password, reset the volume
(loses SQL Server data — the pipeline reloads it from CSV):

```bash
docker compose down
docker volume rm project-101-etl-pipeline_sqlserver_data
docker compose up -d
```

After either fix, **recreate the Airflow containers** so they pick up
the new env vars from `.env`:

```bash
docker compose up -d --force-recreate --no-deps \
  airflow-scheduler airflow-webserver
```

---

## 16. MySQL TRUNCATE Blocked by Foreign Keys

### Symptom

```
sqlalchemy.exc.ProgrammingError: (mysql.connector.errors.ProgrammingError)
1701 (42000): Cannot truncate a table referenced in a foreign key constraint
(`stackoverflow_processed`.`respondent_education`, CONSTRAINT
`respondent_education_ibfk_1`)
```

### Cause

MySQL refuses to TRUNCATE a parent table when any child table has an
FK pointing to it, even if you truncate in the right order. First load
works because tables are empty; every re-run breaks.

### Fix

Toggle `FOREIGN_KEY_CHECKS` off for the truncate statement. It's a
session variable, so the `SET`, `TRUNCATE`, and `SET` back must share
one connection (use `engine.begin()`):

```python
with engine.begin() as conn:
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
    conn.execute(text(f"TRUNCATE TABLE {table_name}"))
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
```

Safe because inserts still run in parent-first order, so FK integrity
is restored naturally once the table is refilled.

---

## 17. NVARCHAR Columns Too Narrow for Multi-Value Data

### Symptom

```
pymssql.exceptions.OperationalError: (2628, b"String or binary data
would be truncated in table 'stackoverflow_raw.dbo.survey_responses_raw',
column 'dev_type'. Truncated value: 'Academic researcher;Data or business
analyst;Data scientist or machine learning specialist;Database...'")
```

### Cause

Stack Overflow survey fields like `DevType`, `Ethnicity`, `Gender`,
`Employment` are semicolon-joined multi-value answers that routinely
exceed 500 characters when a respondent ticks many boxes. The original
schema had `NVARCHAR(500)` — too tight for the data.

### Fix

Widen the multi-value columns in `sql/mssql_schema.sql` to `NVARCHAR(MAX)`.
This is a bronze/raw staging layer — no perf reason to constrain string
length here:

```sql
gender      NVARCHAR(MAX),
ethnicity   NVARCHAR(MAX),
employment  NVARCHAR(MAX),
dev_type    NVARCHAR(MAX),
```

Re-apply the schema (`DROP TABLE IF EXISTS` at the top of the file
makes this safe to re-run).

---

## 18. Transform DataFrame Has Extra Columns

### Symptom

```
(1054, "Unknown column 'job_satisfaction' in 'field list'")
```

The `INSERT INTO fact_survey_responses (..., job_satisfaction, ...)`
fails because `job_satisfaction` doesn't exist — only `job_satisfaction_score`
does.

### Cause

The transform builds score columns from text labels but leaves the
intermediate text columns attached to the DataFrame:

```python
fact['job_satisfaction_score'] = fact['job_satisfaction'].map(satisfaction_map)
# 'job_satisfaction' text column stays in the DataFrame → load fails
```

### Fix

Drop the intermediate columns before writing:

```python
fact = fact.drop(columns=['job_satisfaction', 'career_satisfaction'])
```

**General rule:** `df.to_sql` uses *every column* in the DataFrame as
part of the INSERT. Before any `to_sql` call, the DataFrame schema
must exactly match the target table columns.

---

## 19. Silent Batch-Failure in Load Tasks

### Symptom

Load task shows green in Airflow. Log says `"Successfully loaded: 64,461 rows"`.
But the target table is actually empty (verified by direct query).

### Cause

Per-batch `try/except` caught errors, logged a warning, and continued:

```python
except Exception as batch_error:
    failed_rows += len(batch)
    logger.warning(f"⚠️ Batch {batch_num} failed: {str(batch_error)}")
    continue  # ← swallowed the error
```

If every batch fails (e.g. pandas/SQLAlchemy version mismatch), the
task completes without raising. Empty data propagates downstream
until the validate task finally catches it.

### Fix

Raise a `RuntimeError` at end of the loop if total failure or > threshold:

```python
if loaded_rows == 0 and total_rows > 0:
    raise RuntimeError(
        f"Load failed: 0 of {total_rows:,} rows persisted. "
        f"First batch error: {first_error}"
    )

if failure_pct > FAILURE_THRESHOLD_PCT:
    raise RuntimeError(...)
```

Keep the per-batch warnings for debugging, but exit hard if the
overall result isn't acceptable.

---

## 20. Airflow Scheduler Not Running

### Symptom

Airflow UI shows banner: *"The scheduler does not appear to be running.
The DAGs list may not update, and new tasks will not be scheduled."*
Webserver is reachable, no DAGs show in the list.

### Cause

Single-container setup with `command: bash -c "... airflow webserver & airflow scheduler"`
is fragile — if one process dies the other stays alone, creating the
zombie state seen above. Also common for the command to get mangled
and drop the scheduler entirely.

### Fix

Split into three services sharing the same image. The Airflow docs
recommend this pattern:

```yaml
airflow-init:
  build: .
  environment: &airflow_env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    # ... shared config ...
  volumes: &airflow_volumes
    - ./dags:/opt/airflow/dags
    - ./pipeline:/opt/airflow/pipeline
  command: >
    bash -c "airflow db migrate &&
             (airflow users create --username admin ... || true)"
  restart: "no"

airflow-webserver:
  build: .
  depends_on:
    airflow-init:
      condition: service_completed_successfully
  environment: *airflow_env
  volumes: *airflow_volumes
  ports: ["8080:8080"]
  command: airflow webserver --port 8080
  restart: unless-stopped

airflow-scheduler:
  build: .
  depends_on:
    airflow-init:
      condition: service_completed_successfully
  environment: *airflow_env
  volumes: *airflow_volumes
  command: airflow scheduler
  restart: unless-stopped
```

YAML anchors (`&airflow_env` / `*airflow_env`) share config across the
three services without duplication.

---

## 21. Truncated Downloads Not Detected

### Symptom

```
ERROR - ❌ Download failed: File is not a zip file
```

The download log shows progress stopping at 1% or 2%, then jumping
to `✅ Download complete!` on a 112KB file that should be 9.45MB.

### Cause

`requests.iter_content(...)` ends silently when the TCP connection
drops mid-stream. No exception is raised. The code writes whatever
arrived and declares success. Then `zipfile.ZipFile` correctly
rejects the truncated file.

### Fix

Verify download integrity before extracting:

```python
downloaded_size = os.path.getsize(ZIP_PATH)
if total_size > 0 and downloaded_size < total_size:
    os.remove(ZIP_PATH)
    raise IOError(
        f"Short download: got {downloaded_size:,} of "
        f"{total_size:,} bytes"
    )

import zipfile
if not zipfile.is_zipfile(ZIP_PATH):
    os.remove(ZIP_PATH)
    raise IOError("Downloaded file is not a valid ZIP archive")
```

Raising causes Airflow to retry per the DAG's `retries` setting.

---

## 22. Redundant run_extraction in Downstream Tasks

### Symptom

Every DAG run downloads the 9.45MB Stack Overflow ZIP multiple times.
`load_to_mysql` task takes 10+ minutes even when data hasn't changed,
because it re-runs extract + load_sqlserver + transform before its
actual MySQL work.

### Cause

Downstream task functions called `run_extraction()` and `run_load()`
defensively, on the assumption that each task starts from scratch.
In Airflow each task IS a fresh process, but data already persisted
to SQL Server from the upstream task survives — no need to re-extract.

### Fix

Each task should only do its own stage:

```python
def task_load_sqlserver(**context):
    from extract import extract_survey_data, RAW_DATA_PATH
    from load import run_load
    df = extract_survey_data(RAW_DATA_PATH)  # read cached CSV, no download
    run_load(df)

def task_load_mysql(**context):
    from transform import run_transform
    from load_mysql import run_mysql_load
    transformed = run_transform()  # reads from SQL Server
    run_mysql_load(transformed)
```

Also fix the `check_for_updates` function in `extract.py` — don't
compare remote ZIP size to local CSV size (they'll never match). Use
a marker file storing the last seen `Last-Modified` header instead.

---

## 23. Git Bash Path Conversion Breaks docker exec

### Symptom

```bash
$ docker exec -it project101_sqlserver /opt/mssql-tools18/bin/sqlcmd ...
OCI runtime exec failed: exec failed: unable to start container process:
exec: "C:/Program Files/Git/opt/mssql-tools18/bin/sqlcmd": stat ...:
no such file or directory
```

### Cause

Git Bash on Windows (MINGW64) auto-converts any argument starting with
`/` into a Windows path (`C:/Program Files/Git/opt/...`). The container
path never reaches Docker.

### Fix

Three options, in order of preference:

**1. Leading double-slash** (simplest):
```bash
docker exec -it project101_sqlserver //opt/mssql-tools18/bin/sqlcmd ...
```

**2. Disable path conversion for one command:**
```bash
MSYS_NO_PATHCONV=1 docker exec -it project101_sqlserver \
  /opt/mssql-tools18/bin/sqlcmd ...
```

**3. Shell into the container first, then run absolute paths normally:**
```bash
docker exec -it project101_sqlserver bash
# now inside container, no Git Bash path translation
/opt/mssql-tools18/bin/sqlcmd ...
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
7. **MSSQL/MySQL volumes bake passwords on first boot** — env changes
   don't update them. Either `ALTER LOGIN` in-place or drop the volume.
8. **Fail loud, not silent** — load/insert loops should raise when batches
   fail, not just log warnings. A silent 0-row load fails downstream anyway.
9. **Match DataFrame schema to target table before `to_sql`** — drop any
   intermediate columns, rename to match, check column counts.
10. **Don't trust 18456 Login Failed from SQL Server** — always grep the
    real reason from `/var/opt/mssql/log/errorlog` inside the container.
11. **Bronze/raw staging columns should be generous** — `NVARCHAR(MAX)` is
    fine for multi-value survey fields, no perf cost on a staging table.
12. **Split Airflow into init/webserver/scheduler services** — running both
    webserver and scheduler in one container from a single `command:` is
    fragile. Use YAML anchors to share config.
13. **Check library compatibility matrices before bumping versions** — e.g.
    pandas 2.2 requires SQLAlchemy ≥ 2.0, but Airflow 2.11 requires
    SQLAlchemy < 2.0. Pin `pandas>=2.1,<2.2` on this stack.
14. **When a download "completes" but file is tiny, distrust silent success**
    — verify `Content-Length` matches bytes written AND the file passes a
    format check (e.g. `zipfile.is_zipfile()`) before using it.
15. **Each Airflow task should only do its own stage** — don't defensively
    re-run upstream. Use disk/database state that upstream already persisted.

---

_Last updated: April 2026_
_Project: project-101-etl-pipeline_
