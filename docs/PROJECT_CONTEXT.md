# Project 101 / 102 — Context Primer

A self-contained context document. Paste this at the top of any new AI session (or share with a collaborator) to bootstrap full understanding of the project state without needing to re-derive history.

---

## 1. Author profile

- **Background:** DBA transitioning into cloud data engineering
- **Platform:** Windows 11, Docker Desktop, Git Bash (MINGW64) + PowerShell, VS Code
- **Constraint:** building on free / zero-cost tooling where possible
- **Working style:** prefers hands-on typing for muscle memory; values blunt feedback over fluff; documents failures as well as successes

---

## 2. Project 101 — Local ETL Pipeline (COMPLETE ✅)

End-to-end ETL pipeline for the Stack Overflow 2020 Developer Survey, implementing a full Medallion Architecture (Bronze → Silver → Gold) plus a live Grafana dashboard. Took **5 DAG runs and 23 documented bugs** to reach green.

### 2.1 Architecture

| Layer | Storage | Notes |
|---|---|---|
| Source | CSV | Stack Overflow 2020 survey, ~9.5MB ZIP, 64,461 respondents |
| Bronze (raw) | SQL Server 2022 | `stackoverflow_raw.dbo.survey_responses_raw` |
| Silver (cleaned) | MySQL 8 | `stackoverflow_processed.*` — 5 tables |
| Gold (analytical) | MySQL 8 | `stackoverflow_analytics.*` — star schema (1 fact + 2 dims) |
| Orchestration | Airflow 2.11.1 | LocalExecutor, metadata in MySQL |
| Monitoring | Grafana | Reads from MySQL + SQL Server datasources |
| CI/CD | GitHub Actions | flake8 + pytest + `docker compose config` |

### 2.2 Final row counts (verified loaded end-to-end)

| Table | Rows |
|---|---|
| `stackoverflow_raw.survey_responses_raw` | 64,461 |
| `stackoverflow_processed.respondents` | 64,461 |
| `stackoverflow_processed.respondent_education` | 64,461 |
| `stackoverflow_processed.respondent_compensation` | 63,693 |
| `stackoverflow_processed.respondent_technologies` | 1,157,765 |
| `stackoverflow_processed.respondent_dev_types` | 157,094 |
| `stackoverflow_analytics.dim_developer` | 64,461 |
| `stackoverflow_analytics.dim_geography` | 184 |
| `stackoverflow_analytics.fact_survey_responses` | 64,461 |
| **Total** | **1,636,580** |

### 2.3 Tech stack — exact pins (DO NOT change without reading gotchas)

```
apache-airflow==2.11.1            # NOT 2.8.x (old Python), NOT 3.x (breaks DAG code)
pandas>=2.1,<2.2                  # NOT 2.2+ (silently fails with SQLAlchemy 1.4)
sqlalchemy>=1.4.54,<2.0           # NOT 2.0+ (Airflow 2.11 hard-pins via flask-appbuilder<1.5)
numpy>=1.26,<2.3
pymssql==2.3.13
PyMySQL==1.1.2
pyodbc==5.3.0
great-expectations==0.18.22       # NOT 1.x (breaking API rewrite)
python-dotenv==1.1.0
openpyxl==3.1.5
pytest==8.3.5
pytest-cov==5.0.0
loguru==0.7.3
tqdm==4.67.3
colorama==0.4.6
```

Base Docker image: `apache/airflow:2.11.1-python3.12`

### 2.4 Repo structure

```
project-101-etl-pipeline/
├── .env                           # passwords, secrets — NOT in git
├── .env.example                   # template
├── Dockerfile                     # custom Airflow image with project deps
├── docker-compose.yml             # 6 services (see 2.6)
├── requirements.txt
├── README.md
├── dags/
│   └── etl_pipeline.py            # The single DAG: project101_etl_pipeline (8 tasks)
├── pipeline/
│   ├── extract.py                 # Download + parse CSV (with marker file for re-run skip)
│   ├── load.py                    # Bronze: load to SQL Server, raises on >5% batch failure
│   ├── transform.py               # Build silver tables + dim_*/fact_*; populates FK keys
│   └── load_mysql.py              # Silver/Gold: load to MySQL; toggles FK_CHECKS for truncate
├── sql/
│   ├── mssql_schema.sql           # Bronze table DDL (multi-value cols are NVARCHAR(MAX))
│   ├── mysql_schema.sql           # Silver + Gold DDL — auto-runs on first MySQL boot
│   └── security_setup.sql
├── tests/                         # pytest suite
├── docs/
│   ├── TROUBLESHOOTING.md         # 23 issues with symptom/cause/fix — REQUIRED reading
│   ├── CHANGES_2026-04-18.txt     # early-debugging change log
│   └── PROJECT_CONTEXT.md         # this file
├── .github/workflows/
│   └── tests.yml                  # CI: flake8 (pipeline/), pytest, compose validate
└── monitoring/
    └── grafana/                   # dashboards when exported (provisioning planned)
```

### 2.5 DAG flow

```
start → extract_data → load_to_sqlserver → transform_data → load_to_mysql → validate_pipeline → notify_success → end
```

**Important rule:** each task does ONLY its own stage. Do NOT have downstream tasks re-run `run_extraction()` or `run_load()` — that bug previously caused redundant 9.5MB downloads and 10+ minute task durations on every run. Each task reads disk/DB state that the previous task persisted.

### 2.6 Container layout (after the init/webserver/scheduler split)

| Service | Container name | Role |
|---|---|---|
| `sqlserver` | `project101_sqlserver` | MSSQL 2022 Bronze layer |
| `mysql` | `project101_mysql` | MySQL 8 Silver/Gold + Airflow metadata |
| `grafana` | `project101_grafana` | Dashboard UI at :3000 |
| `airflow-init` | `project101_airflow_init` | One-shot `db migrate` + admin user, exits |
| `airflow-webserver` | `project101_airflow_webserver` | Airflow UI at :8080 |
| `airflow-scheduler` | `project101_airflow_scheduler` | DAG parser + task executor |

YAML anchors (`&airflow_env` / `*airflow_env`) share env + volumes across the 3 Airflow services.

### 2.7 Verified reference credentials (as of last green run)

| Service | User | Password |
|---|---|---|
| SQL Server | `sa` | `Pro101Mssql123` |
| MySQL root | `root` | `pro101mysql123` |
| MySQL app user | `project101_user` | `pro101mysql123` |
| Airflow admin | `admin` | `admin123` |
| Grafana admin | `admin` | `admin123` |

`project101_user` has `ALL PRIVILEGES` on `stackoverflow_processed` and `stackoverflow_analytics`, plus `SELECT` on `airflow_metadata` (granted for Grafana ops dashboards).

### 2.8 Critical gotchas (top 5 — full list in `docs/TROUBLESHOOTING.md`)

1. **Pandas 2.2 silently breaks with SQLAlchemy 1.4.** Pandas falls back to DBAPI mode; `df.to_sql(engine)` errors with `'Engine' has no attribute 'cursor'`. Per-batch try/except swallows it → empty tables, "successful" task. **Fix:** pin `pandas<2.2` OR `sqlalchemy>=2.0` (Airflow 2.11 forbids the latter, so use the former).
2. **MSSQL 18456 "Login Failed" often masks "database does not exist."** Always grep `/var/opt/mssql/log/errorlog` inside the container for the real reason before chasing password issues.
3. **MSSQL volume bakes the SA password on FIRST boot only.** Changing `MSSQL_SA_PASSWORD` in `.env` later does nothing. Either `ALTER LOGIN sa` in-place or drop the volume.
4. **MySQL TRUNCATE blocked by FK constraints** on every re-run. Toggle `SET FOREIGN_KEY_CHECKS = 0` around the TRUNCATE. Same connection (use `engine.begin()`).
5. **Git Bash on Windows mangles container paths.** `docker exec ... /opt/mssql-tools18/bin/sqlcmd` becomes `C:/Program Files/Git/opt/...`. **Fix:** leading `//` or `MSYS_NO_PATHCONV=1`.

### 2.9 Quick command reference

```bash
# Up / down
docker compose up -d
docker compose down
docker compose down -v             # NUKES volumes — resets passwords + data

# Status / logs
docker compose ps
docker compose logs airflow-scheduler --tail 50

# Exec into databases
docker exec -it project101_mysql mysql -uroot -ppro101mysql123
docker exec -it project101_sqlserver //opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'Pro101Mssql123' -C

# Airflow ops
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags trigger project101_etl_pipeline

# Reading the real MSSQL error log
MSYS_NO_PATHCONV=1 docker exec project101_sqlserver \
  sh -c "grep -a 'Login failed' /var/opt/mssql/log/errorlog | tail -5"
```

### 2.10 What's NOT done in Project 101 (open follow-ups)

- `education_key` and `compensation_key` columns on `fact_survey_responses` are still NULL — no `dim_education` / `dim_compensation` tables exist yet. Not blocking; analytics queries route through silver tables.
- Backups: nothing automated. No `mysqldump` / `BACKUP DATABASE` task in the DAG yet.
- Grafana dashboards live in the `grafana_data` Docker volume — not exported to `monitoring/grafana/dashboards/` JSON yet, so they'd be lost on `docker compose down -v`.
- Project 101 ops dashboard (DAG status, slow queries, freshness) is partially started — first panel built, the rest is the user's exercise to type/learn.

---

## 3. Project 102 — AWS Cloud-Native Migration (PLANNED, NOT STARTED)

Same pipeline, re-architected as cloud-native AWS — explicitly NOT a lift-and-shift. **Target cost: under $3/month.**

### 3.1 Stack mapping

| Project 101 (local) | Project 102 (AWS) | Why |
|---|---|---|
| CSV file | S3 Bronze bucket | object storage primitive |
| SQL Server (Bronze) | S3 Bronze (Parquet) | RDS SQL Server isn't free-tier; S3 is |
| Python ETL | AWS Glue Python Shell jobs | serverless, ~$0.01 per run at this size |
| MySQL (Silver/Gold) | S3 Silver/Gold (Parquet) + Glue Catalog | RDS free tier expires at 12mo |
| Airflow | EventBridge Scheduler + Step Functions | MWAA = $50–80/mo, this is free |
| Grafana (local Docker) | Grafana (local) → Athena datasource | Managed Grafana = $9/user/mo |
| `.env` | AWS Secrets Manager | rotation, audit |
| `docker-compose.yml` | Terraform or AWS CDK | Infrastructure as Code from day one |
| GitHub Actions (tests) | GitHub Actions: `terraform plan/apply` | same pattern, cloud target |

### 3.2 Phase plan

| Phase | Focus | Est. cost |
|---|---|---|
| 0 | $5 Budget alarm, Terraform skeleton, IAM + tagging baseline | Free |
| 1 | S3 buckets (3-tier), Secrets Manager for credentials | Free |
| 2 | Upload CSV → S3 Bronze, Glue Crawler, query in Athena | Free |
| 3 | Glue job: Bronze → Silver Parquet (port `transform.py`) | ~$0.01/run |
| 4 | Glue job: Silver → Gold Parquet | ~$0.01/run |
| 5 | Step Functions state machine + EventBridge daily trigger | Free |
| 6 | CloudWatch dashboards + SNS failure email alerts | Free |
| 7 | Local Grafana → Athena datasource, rebuild dashboards | Free |
| 8 | GitHub Actions CI/CD: PR → `terraform plan`, merge → `apply` | Free |
| 9 | *(optional)* MWAA experiment for 1 week, then destroy | ~$20 |

### 3.3 Cost traps (set/avoid these on day one)

- **AWS Budgets alarm at $5** before anything else is provisioned. Free, non-negotiable insurance.
- **NAT Gateway is $32/mo minimum.** Use VPC Gateway Endpoint for S3 (free) or run jobs in public subnets (acceptable for non-prod).
- **CloudWatch log retention defaults to "Never expire"** — set every log group to 7 days.
- **Practice `terraform destroy`** at the end of every work session until you fully trust it.
- **Free tier expirations:** RDS / EC2 are 12 months. S3 + Glue + Lambda + Step Functions are forever-cheap. Stay in the latter where possible.

### 3.4 Production-grade additions worth including

- Tagging strategy (`Project=project102`, `Environment=dev`, `Owner=...`) on every resource
- Glue Data Quality rules (free) on Silver tables
- Glue Schema Registry (free, up to 100 schemas) for Parquet schema enforcement
- SNS topic subscribed to email for Step Functions failure events
- Terraform state in S3 + DynamoDB lock (~$0/month at this scale)

---

## 4. How to use this document

When starting a new AI session about this project, paste this entire file at the top with a note like:

> Here's the full context for the project I'm working on. Read this first, then I'll tell you what I want to work on next.

Then state the specific task. The AI will then have:
- Project identity, current state, and constraints
- Exact pinned tech-stack versions (these matter — see §2.8)
- Architecture, DAG flow, container layout
- Known landmines with pointers to detail
- The Project 102 roadmap

When continuing on Project 102 specifically, also mention:
- Whether AWS infrastructure has been provisioned yet (default: nothing exists)
- Whether the $5 Budget alarm is set
- Which phase you want to work on

---

## 5. Files to read for full detail

| File | What's in it |
|---|---|
| `docs/TROUBLESHOOTING.md` | All 23 issues with symptom / cause / fix |
| `docs/CHANGES_2026-04-18.txt` | Early-debugging change log |
| `README.md` | Project overview for newcomers |
| `requirements.txt`, `Dockerfile` | Exact pinned versions |
| `dags/etl_pipeline.py` | DAG structure and task contracts |
| `pipeline/extract.py` | Download with marker-file freshness check + size/zip integrity check |
| `pipeline/load.py` | Bronze loader with fail-loud batch threshold |
| `pipeline/transform.py` | Silver cleaning, dim/fact builds, FK key population |
| `pipeline/load_mysql.py` | Silver/Gold loader with FK_CHECKS toggle |
| `sql/mssql_schema.sql` | Bronze DDL (multi-value cols = NVARCHAR(MAX)) |
| `sql/mysql_schema.sql` | Silver + Gold DDL, auto-runs on fresh MySQL volume |
| `docker-compose.yml` | Full container topology (6 services, YAML anchors for Airflow) |

---

_Last updated: 2026-04-25_
_Status: Project 101 complete and green; Project 102 planned, not started._
_Maintainer: Thierry — github.com/Thierry0326/project-101-etl-pipeline_
