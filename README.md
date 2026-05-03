# globalpartners-data-pipeline
unified data view that helps us understand customer behavior, spending patterns, and overall business performance across all restaurant locations and platforms
# GlobalPartners Data Pipeline

AWS-native data engineering pipeline for GlobalPartners Restaurant Group.

## Architecture
SQL Server (RDS) → Bronze S3 → Silver S3 → Gold S3 → Redshift Serverless → Streamlit

## Stack
- Ingestion: AWS Glue 4.0 (PySpark), JDBC watermark-based incremental loads
- Storage: Amazon S3 (Parquet, partitioned), Glue Data Catalog
- Orchestration: Amazon MWAA (Airflow)
- Serving: Amazon Redshift Serverless
- Visualization: Streamlit on ECS Fargate
- CI/CD: GitHub Actions → AWS CodePipeline

## Layers
| Layer | Location | Status |
|---|---|---|
| Bronze | s3://gp-data-lake-dev/bronze/ | ✅ Complete |
| Silver | s3://gp-data-lake-dev/silver/ | ⏳ In progress |
| Gold | s3://gp-data-lake-dev/gold/ | ⏳ Pending |

## Setup
```bash
conda create -n gp-pipeline python=3.11 -y
conda activate gp-pipeline
pip install -r requirements-dev.txt
```
