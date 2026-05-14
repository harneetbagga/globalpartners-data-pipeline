# GlobalPartners Restaurant Analytics Pipeline

> End-to-end AWS data engineering pipeline for GlobalPartners Restaurant Group — ingesting transactional POS data from SQL Server, transforming through a Bronze/Silver/Gold medallion architecture, and serving business intelligence through an interactive Streamlit dashboard.

[![Live Dashboard](https://img.shields.io/badge/Dashboard-Live-brightgreen)](https://globalpartners-analytics.streamlit.app)
[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://www.python.org/)
[![AWS Glue](https://img.shields.io/badge/AWS-Glue%204.0-orange)](https://aws.amazon.com/glue/)
[![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-black)](https://github.com/features/actions)

---

## 🍽️ Live Dashboard

**[https://globalpartners-analytics.streamlit.app](https://globalpartners-analytics.streamlit.app)**

7 interactive pages covering Customer Analytics, Churn & Retention, Sales Trends, Location Performance, Loyalty Analysis, and Discount Effectiveness — all backed by live Athena queries against Gold S3 data.

---

## Architecture

```
SQL Server (RDS)
    │
    │  JDBC incremental extract
    │  Watermark tracking via DynamoDB
    ▼
Bronze S3  (raw Parquet, partitioned year/month/day)
    │
    │  PySpark transformations
    │  DQ flagging · Deduplication · Type casting
    ▼
Silver S3  (clean Parquet, customer eligibility flag)
    │
    │  7 parallel Gold jobs
    ▼
Gold S3  (CLV · RFM · Churn · Sales Trends · Loyalty · Location · Discount)
    │
    │  Athena queries
    ▼
Streamlit Dashboard  (7 pages · live analytics)
```

Orchestrated end-to-end by a **Glue Workflow** with conditional triggers — each layer starts only after the previous layer's crawler confirms the catalog is updated.

---

## Stack

| Layer | Technology |
|---|---|
| Source | SQL Server on AWS RDS |
| Ingestion | AWS Glue 4.0 (PySpark), JDBC watermark-based incremental loads |
| Storage | Amazon S3 (Parquet + Snappy), partitioned by year/month/day |
| Catalog | AWS Glue Data Catalog (gp_bronze, gp_silver, gp_gold) |
| Transformation | AWS Glue PySpark jobs |
| Query Engine | Amazon Athena |
| Orchestration | AWS Glue Workflows with conditional triggers |
| Watermark Tracking | Amazon DynamoDB |
| Secrets Management | AWS Secrets Manager |
| Visualization | Streamlit (Athena-backed, 7 pages) |
| CI/CD | GitHub Actions → S3 sync on merge to main |
| IaC | AWS CDK (Python) |

---

## Pipeline Layers

### Bronze — Raw Ingestion
- Incremental load via JDBC watermark (`creation_time_utc`) tracked in DynamoDB
- `order_items`: incremental by business timestamp
- `order_item_options`: full reload every run (no business timestamp)
- `date_dim`: full reload every run (reference table)
- Partitioned: `year/month/day` for `order_items`, flat for others

### Silver — Cleaned and Validated
- **DQ flagging** — `CLEAN`, `NULL_USER_ID`, `NULL_CARD_NUMBER`, `NULL_PRICE`, `NEGATIVE_PRICE`
- **Hard drop** only on null `lineitem_id` or `order_id` — dropped rows written to DLQ
- **Keep with flag** — null `user_id` (~17K rows) and null `printed_card_number` (~139K rows) preserved for revenue/location analysis
- **Deduplication** — `lineitem_id` PK for `order_items`; composite `(lineitem_id, option_group_name, option_name)` for options
- **Customer eligibility flag** — `eligible_for_customer_metrics = true` when `user_id IS NOT NULL`

| Table | Bronze Rows | Silver Rows | Dropped/Deduped |
|---|---|---|---|
| order_items | 203,519 | 203,518 | 1 hard dropped |
| order_item_options | 193,017 | 190,718 | 2,299 deduped |
| date_dim | 365 | 365 | — |

### Gold — Business Metrics (7 Parallel Jobs)

| Job | Output Tables | Description |
|---|---|---|
| `gold_clv` | `clv` | Customer Lifetime Value — revenue bands, churn risk, loyalty comparison |
| `gold_rfm` | `rfm` | RFM segmentation — quintile scoring, 8 customer segments |
| `gold_sales_trends` | `sales_trends_daily/weekly/monthly` | Revenue trends enriched with date_dim (holidays, weekends) |
| `gold_churn_indicators` | `churn_indicators` | Activity profiles — order gaps, spend trends, risk tags |
| `gold_loyalty_comparison` | `loyalty_summary/by_category/monthly_trend` | Loyalty vs non-loyalty behaviour comparison |
| `gold_location_performance` | `location_performance/monthly/top_items` | Restaurant rankings, MoM growth, top items per location |
| `gold_discount_effectiveness` | `discount_order_summary/by_category/customer_behaviour` | Discount impact on basket size and customer segments |

---

## Key Business Insights

| Metric | Value |
|---|---|
| Unique customers | 20,174 |
| HIGH CLV customers | 4,034 — avg $1,821 net revenue |
| Customer-eligible rows | 185,710 (91.3% of order items) |
| Loyalty insight | Non-loyalty avg CLV $464 vs loyalty $192 — but loyalty members order 3.3× more frequently |
| CLV outlier | One customer with $2.5M revenue — likely corporate/catering account |

---

## Repo Structure

```
globalpartners-data-pipeline/
│
├── .github/
│   └── workflows/
│       ├── ci.yml              ← lint, test, security scan on PRs
│       └── cd.yml              ← deploy Glue scripts to S3 on merge to main
│
├── glue_jobs/
│   ├── bronze_ingestion.py
│   ├── silver_transform.py
│   ├── silver_functions.py     ← pure PySpark functions (unit testable)
│   ├── gold_clv.py
│   ├── gold_rfm.py
│   ├── gold_sales_trends.py
│   ├── gold_churn_indicators.py
│   ├── gold_loyalty_comparison.py
│   ├── gold_location_performance.py
│   └── gold_discount_effectiveness.py
│
├── streamlit_app/
│   ├── app.py                  ← main entry point
│   ├── requirements.txt
│   ├── .python-version         ← pins Python 3.11
│   ├── pages/
│   │   ├── 1_overview.py
│   │   ├── 2_customers.py
│   │   ├── 3_churn.py
│   │   ├── 4_sales_trends.py
│   │   ├── 5_locations.py
│   │   ├── 6_loyalty.py
│   │   └── 7_discounts.py
│   ├── utils/
│   │   ├── __init__.py
│   │   └── athena.py           ← Athena query helper with 1-hour caching
│   └── .streamlit/
│       ├── config.toml
│       └── secrets.toml.example
│
├── infra/
│   ├── glue_workflow.json      ← workflow config documentation
│   ├── glue_crawlers.json      ← crawler config documentation
│   ├── deploy_workflow.sh      ← recreate Glue Workflow from scratch
│   └── cdk/                   ← AWS CDK IaC (Python)
│       ├── app.py
│       └── globalpartners/
│           └── globalpartners_stack.py
│
├── tests/
│   └── unit/
│       └── test_silver_transform.py
│
├── scripts/
│   └── csv_data_upload.py
│
├── airflow_dags/               ← placeholder for future MWAA DAGs
├── requirements-dev.txt
└── README.md
```

---

## Glue Workflow — Orchestration DAG

```
[Schedule 2AM UTC]
       ↓
[gp-bronze-ingestion]
       ↓
[gp-bronze-crawler]
       ↓
[gp-silver-transform]
       ↓
[gp-silver-crawler]
       ↓
┌──────┬──────┬──────────────┬─────────┬──────────┬──────────┬──────────┐
↓      ↓      ↓              ↓         ↓          ↓          ↓
clv   rfm  sales-trends  churn  loyalty  location  discount
└──────┴──────┴──────────────┴─────────┴──────────┴──────────┴──────────┘
                             ↓
             [gp-gold-crawler + gp-gold-churn-crawler]
```

---

## CI/CD Pipeline

| Trigger | Workflow | Steps |
|---|---|---|
| PR to main | `ci.yml` | Black format check → Flake8 lint → Bandit security scan → pytest unit tests |
| Merge to main | `cd.yml` | Sync Glue scripts to S3 → Sync Airflow DAGs to S3 |

---

## AWS Resources

| Resource | Name | Purpose |
|---|---|---|
| S3 Bucket | `gp-data-lake-dev` | Data lake — Bronze/Silver/Gold/Scripts/DLQ |
| DynamoDB | `gp_watermarks` | Incremental load watermark tracking |
| Secrets Manager | `gp/sqlserver/pipeline` | RDS credentials |
| IAM Role | `GlueGPExecutionRole` | Glue job execution permissions |
| Glue Databases | `gp_bronze`, `gp_silver`, `gp_gold` | Data Catalog |
| Glue Crawlers | 4 crawlers (bronze/silver/gold/churn) | Schema detection and catalog update |
| Glue Jobs | 9 jobs (1 bronze + 1 silver + 7 gold) | ETL transformations |
| Glue Workflow | `gp-data-pipeline` | End-to-end orchestration |
| RDS | SQL Server Express, db.t3.micro | Source operational database |

---

## Local Development Setup

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/globalpartners-data-pipeline.git
cd globalpartners-data-pipeline

# Create conda environment
conda create -n gp-pipeline python=3.11 -y
conda activate gp-pipeline

# Install dev dependencies
pip install -r requirements-dev.txt

# Run unit tests
pytest tests/unit/ -v --cov=glue_jobs --cov-report=term-missing

# Run Streamlit dashboard locally
cd streamlit_app
pip install -r requirements.txt
streamlit run app.py
```

### AWS Credentials for Local Dashboard

Create `streamlit_app/.streamlit/secrets.toml` (never commit this file):

```toml
[aws]
access_key_id     = "YOUR_AWS_ACCESS_KEY_ID"
secret_access_key = "YOUR_AWS_SECRET_ACCESS_KEY"
```

---

## Deploying Glue Scripts

Scripts are automatically deployed to S3 via `cd.yml` on every merge to `main`.

For manual deployment:

```bash
aws s3 sync glue_jobs/ s3://gp-data-lake-dev/scripts/ \
  --exclude "*.pyc" \
  --delete
```

---

## Infrastructure as Code

The CDK stack in `infra/cdk/` defines all AWS resources — S3 bucket with lifecycle policies, DynamoDB, IAM roles, Glue databases, crawlers, and all 9 Glue jobs.

```bash
cd infra/cdk
pip install aws-cdk-lib constructs
cdk synth    # generate CloudFormation template
cdk deploy   # deploy to AWS (requires admin permissions)
```

---

## Data Quality Strategy

| Decision | Implementation |
|---|---|
| Only hard drop on broken PK | `lineitem_id IS NULL` or `order_id IS NULL` → DLQ |
| Keep null user_id rows | Flagged `NULL_USER_ID`, excluded from customer metrics only |
| Keep null card numbers | Flagged `NULL_CARD_NUMBER`, non-critical loyalty field |
| Customer eligibility | `eligible_for_customer_metrics` boolean — Gold jobs filter on this |
| DLQ for traceability | Hard-dropped rows written to `s3://gp-data-lake-dev/dlq/` |

---

## Architectural Decisions

| Decision | Choice | Rationale |
|---|---|---|
| ETL vs ELT | ETL | Serving layer is S3 + Athena, not a warehouse — ELT requires in-warehouse SQL compute |
| Batch vs Streaming | Batch (daily) | Daily grain is sufficient for churn/CLV/loyalty decisions — streaming adds complexity with no business benefit |
| Glue vs EMR | Glue serverless | At 200K rows, EMR cluster overhead is unjustifiable — Glue pays only per execution |
| Parquet vs Delta Lake | Parquet | Delta requires external JARs on Glue, partial Athena compatibility — full partition overwrites make MERGE unnecessary |
| Athena vs Redshift | Athena | At 200K rows, performance difference is negligible — Redshift Serverless costs ~$175/month vs Athena at ~$0 |
| Glue Workflows vs MWAA | Glue Workflows | Pipeline is entirely Glue-native — MWAA at ~$360/month is unjustifiable when Workflows solves the same problem for free |

---

## Idempotency

| Layer | Idempotent? | Notes |
|---|---|---|
| Bronze (full reload tables) | ✅ Yes | `overwrite` mode — safe to re-run |
| Bronze (incremental) | ⚠️ Mostly | Watermark update after write — gap risk if job fails between write and watermark update |
| Silver | ✅ Yes | `overwrite` with dynamic partition mode + deduplication |
| Gold | ✅ Yes | `overwrite` partitioned by `snapshot_date` |
| Crawlers | ✅ Yes | Re-running always produces correct catalog state |

---

## License

This project is built for portfolio and educational purposes using synthetic data representing a fictional restaurant group.
