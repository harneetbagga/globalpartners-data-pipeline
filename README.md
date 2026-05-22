# GlobalPartners Restaurant Analytics Pipeline

> End-to-end AWS data engineering pipeline for GlobalPartners Restaurant Group — ingesting transactional POS data from SQL Server via parallel per-table Bronze jobs, transforming through a Bronze/Silver/Gold medallion architecture, and serving business intelligence through an interactive Streamlit dashboard.

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
    │  3 parallel per-table JDBC extracts
    ├──────────────────────────────────────────┐
    │                                          │
    ▼                                          ▼
gp-bronze-order-items       gp-bronze-order-item-options    gp-bronze-date-dim
(incremental, watermark)    (full reload)                   (full reload)
    │                                          │
    │  Each Bronze job independently           │
    │  triggers its own Silver job             │
    ▼                                          ▼
gp-silver-order-items       gp-silver-order-item-options    gp-silver-date-dim
(DQ + dedup + eligibility)  (DQ + composite dedup)          (type casting)
    │                                          │
    └──────── Silver Crawler (AND: all 3 Silver SUCCEEDED) ──┘
                                │
                                ▼
    ┌───────┬───────┬─────────────┬──────────┬──────────┬──────────┬──────────┐
    ▼       ▼       ▼             ▼          ▼          ▼          ▼
   clv     rfm  sales-trends  churn    loyalty   location  discount
    └───────┴───────┴─────────────┴──────────┴──────────┴──────────┴──────────┘
                                │
                     Gold Crawlers (AND: all 7 SUCCEEDED)
                                │
                                ▼
                    Athena → Streamlit Dashboard
```

**Fault isolation:** A failure in one Bronze table job does not block the other two. Each table runs its complete Bronze → Silver pipeline independently. The Silver crawler AND trigger ensures Gold always runs on a complete Silver layer.

**Alerting:** Two independent layers — CloudWatch Metric Alarm (primary, fires within 2 min) + Glue Workflow failure trigger → SNS rich notification (secondary, with recovery commands).

---

## Stack

| Layer | Technology |
|---|---|
| Source | SQL Server on AWS RDS |
| Ingestion | AWS Glue 4.0 (PySpark) — 3 parallel per-table Bronze jobs |
| Storage | Amazon S3 (Parquet + Snappy) — partitioned by year/month/day |
| Catalog | AWS Glue Data Catalog (gp_bronze, gp_silver, gp_gold) |
| Transformation | AWS Glue PySpark jobs — 3 Silver + 7 Gold |
| Query Engine | Amazon Athena |
| Orchestration | AWS Glue Workflows — 9 conditional triggers |
| Watermark Tracking | Amazon DynamoDB |
| Secrets Management | AWS Secrets Manager |
| Alerting | CloudWatch Metric Alarm + SNS topic gp-pipeline-alerts |
| Visualization | Streamlit (Athena-backed, 7 pages) |
| CI/CD | GitHub Actions → S3 sync on merge to main |
| IaC | AWS CDK (Python) |

---

## Pipeline Layers

### Bronze — Per-Table Ingestion Jobs

| Glue Job | Table | Load Strategy | Partition | Rows |
|---|---|---|---|---|
| gp-bronze-order-items | order_items | Incremental (watermark) | year/month/day | 203,519 |
| gp-bronze-order-item-options | order_item_options | Full reload | Flat | 193,017 |
| gp-bronze-date-dim | date_dim | Full reload | Flat | 365 |

### Silver — Per-Table Transform Jobs

Each Silver job is triggered independently by its Bronze job's success. Silver crawler fires only when **all 3** Silver jobs SUCCEED.

| Glue Job | Trigger Condition | Key Transforms |
|---|---|---|
| gp-silver-order-items | gp-bronze-order-items SUCCEEDED | DQ flagging, lineitem dedup, eligibility flag |
| gp-silver-order-item-options | gp-bronze-order-item-options SUCCEEDED | DQ flagging, composite key dedup |
| gp-silver-date-dim | gp-bronze-date-dim SUCCEEDED | Type casting, date enrichment |

### Gold — 7 Parallel Business Metric Jobs

All 7 jobs run in parallel after the Silver crawler completes.

| Job | Output Tables | Filter |
|---|---|---|
| gp-gold-clv | clv | eligible_for_customer_metrics |
| gp-gold-rfm | rfm | eligible_for_customer_metrics |
| gp-gold-sales-trends | sales_trends_daily/weekly/monthly | All rows |
| gp-gold-churn-indicators | churn_indicators | eligible_for_customer_metrics |
| gp-gold-loyalty | loyalty_summary/by_category/monthly_trend | All rows |
| gp-gold-location | location_performance/monthly/top_items | All rows |
| gp-gold-discount | discount_order_summary/by_category/customer_behaviour | All rows |

---

## Error Handling and Alerting

### Retry Configuration
All 13 pipeline jobs have **MaxRetries=1** and per-job timeouts (10-45 min). One retry handles transient failures. Second failure triggers human intervention.

### Two-Layer Alerting

| Layer | Mechanism | Speed | Message |
|---|---|---|---|
| Primary | CloudWatch Metric Alarm → SNS | 1-2 min | Generic alarm format |
| Secondary | Glue Workflow failure trigger → gp-send-failure-alert → SNS | 5-10 min | Rich — recovery commands + dashboard URL |

Both layers publish to SNS topic **gp-pipeline-alerts**. The CloudWatch layer is independent of the Glue Workflow — it monitors at the metric level and fires even if the workflow itself has an issue.

### Recovery
With per-table jobs, recovery is scoped to the failed table only:
```bash
# Only re-run the failed table's job chain
aws glue start-job-run --job-name gp-bronze-order-items
aws glue start-job-run --job-name gp-silver-order-items
# Then Silver crawler once all 3 Silver jobs show SUCCEEDED
aws glue start-crawler --name gp-silver-crawler
```

---

## Key Business Insights

| Metric | Value |
|---|---|
| Unique customers | 20,174 |
| HIGH CLV customers | 4,034 — avg $1,821 net revenue |
| Customer-eligible rows | 185,710 (91.3% of order items) |
| Loyalty insight | Non-loyalty avg CLV $464 vs loyalty $192 — loyalty members order 3.3× more |

---

## Repo Structure

```
globalpartners-data-pipeline/
│
├── .github/workflows/
│   ├── ci.yml              ← lint, test, security scan on PRs
│   └── cd.yml              ← deploy all Glue scripts to S3 on merge to main
│
├── glue_jobs/
│   ├── bronze_order_items.py
│   ├── bronze_order_item_options.py
│   ├── bronze_date_dim.py
│   ├── silver_order_items.py
│   ├── silver_order_item_options.py
│   ├── silver_date_dim.py
│   ├── silver_functions.py
│   ├── gold_clv.py
│   ├── gold_rfm.py
│   ├── gold_sales_trends.py
│   ├── gold_churn_indicators.py
│   ├── gold_loyalty_comparison.py
│   ├── gold_location_performance.py
│   ├── gold_discount_effectiveness.py
│   └── send_failure_alert.py
│
├── streamlit_app/
│   ├── app.py
│   ├── requirements.txt
│   ├── .python-version
│   ├── pages/
│   ├── utils/
│   └── .streamlit/
│
├── infra/
│   ├── glue_workflow.json
│   ├── deploy_workflow.sh
│   └── cdk/
│
└── README.md
```

---

## Glue Workflow — Orchestration DAG

```
[Schedule 2AM UTC]
        ↓
┌─────────────────────────────────────────────────────────┐  parallel
│ gp-bronze-order-items                                   │
│ gp-bronze-order-item-options                            │
│ gp-bronze-date-dim                                      │
└─────────────────────────────────────────────────────────┘
        ↓ each triggers its own Silver independently
┌─────────────────────────────────────────────────────────┐  parallel
│ gp-silver-order-items    (when bronze-order-items SUCCEEDED)
│ gp-silver-order-item-options
│ gp-silver-date-dim
└─────────────────────────────────────────────────────────┘
        ↓ AND — all 3 Silver SUCCEEDED
gp-silver-crawler
        ↓
┌───────────────────────────────────────────────────────────────┐  parallel
│ gp-gold-clv │ gp-gold-rfm │ gp-gold-sales-trends             │
│ gp-gold-churn │ gp-gold-loyalty │ gp-gold-location │ gp-gold-discount │
└───────────────────────────────────────────────────────────────┘
        ↓ AND — all 7 SUCCEEDED
gp-gold-crawler + gp-gold-churn-crawler

On ANY job failure → gp-trigger-failure-alert → gp-send-failure-alert → SNS
CloudWatch independently monitors all jobs → alarm fires within 5 min of any failure
```

---

## Architectural Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Per-table Bronze + Silver jobs | 3+3 independent jobs | Fault isolation — one table failure does not block others |
| ETL vs ELT | ETL | Serving layer is S3+Athena, not a warehouse |
| Batch vs Streaming | Daily batch | Daily grain sufficient — streaming adds complexity with no business benefit |
| Glue vs EMR | Glue serverless | At 200K rows EMR overhead unjustifiable |
| Parquet vs Delta Lake | Native Parquet | Delta JARs on Glue 4.0, full overwrites make MERGE unnecessary |
| Athena vs Redshift | Athena | ~$0 vs ~$175/month at this scale |
| Glue Workflows vs MWAA | Glue Workflows | Free, Glue-native; MWAA ~$360/month |
| Two-layer alerting | CloudWatch + Glue trigger | Defense-in-depth — CloudWatch fires independently of workflow state |

---

## Idempotency

| Layer | Idempotent? | Notes |
|---|---|---|
| Bronze order_items | ⚠️ Mostly | Watermark risk if job fails between write and watermark update |
| Bronze options + date_dim | ✅ Yes | Full reload overwrite — always safe |
| Silver (all 3 jobs) | ✅ Yes | Overwrite + dedup — clean output every run |
| Gold (all 7 jobs) | ✅ Yes | Overwrite by snapshot_date |
| Crawlers | ✅ Yes | Re-running always produces correct catalog state |

---

## Local Development

```bash
git clone https://github.com/YOUR_USERNAME/globalpartners-data-pipeline.git
cd globalpartners-data-pipeline
conda create -n gp-pipeline python=3.11 -y
conda activate gp-pipeline
pip install -r requirements-dev.txt
pytest tests/unit/ -v

cd streamlit_app
pip install -r requirements.txt
streamlit run app.py
```

---

## License

Portfolio and educational project using synthetic data.
