import sys
import json
import boto3
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ── Init ───────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc   = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job   = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ── Config ─────────────────────────────────────────────────────────────────
SECRET_NAME  = "gp/sqlserver/pipeline"
BRONZE_PATH  = "s3://gp-data-lake-dev/bronze"
TEMP_PATH    = "s3://gp-data-lake-dev/temp"
REGION       = "us-east-1"
DDB_TABLE    = "gp_watermarks"
RUN_TS       = datetime.now(timezone.utc)
PARTITION_DT = RUN_TS.strftime("%Y-%m-%d")

# ── Fetch credentials from Secrets Manager ─────────────────────────────────
def get_secret(secret_name, region):
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

secret = get_secret(SECRET_NAME, REGION)

JDBC_URL = (
    f"jdbc:sqlserver://{secret['host']}:{secret['port']};"
    f"databaseName={secret['dbname']};"
    f"encrypt=true;trustServerCertificate=true;"
)
JDBC_PROPS = {
    "user":                 secret["username"],
    "password":             secret["password"],
    "driver":               "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "fetchsize":            "1000",
}

# ── Watermark helpers ──────────────────────────────────────────────────────
def get_watermark(table_name):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    table = ddb.Table(DDB_TABLE)
    response = table.get_item(Key={"table_name": table_name})
    return response["Item"]["last_loaded_ts"]

def update_watermark(table_name, new_ts):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    table = ddb.Table(DDB_TABLE)
    table.put_item(Item={
        "table_name":     table_name,
        "last_loaded_ts": new_ts,
    })
    print(f"  Watermark updated → {table_name}: {new_ts}")

# ── Generic incremental load function ─────────────────────────────────────
def incremental_load(table_name, watermark_col, partition_col=None):
    print(f"\n{'='*60}")
    print(f"Loading: {table_name}")

    # 1. Get last watermark
    last_ts = get_watermark(table_name)
    print(f"  Watermark (last loaded): {last_ts}")

    # 2. Build query — only pull records newer than watermark
    query = f"""
        (SELECT *
            FROM   dbo.{table_name}
            WHERE  {watermark_col} > CONVERT(datetime2, '{last_ts}', 127)
        ) AS t
    """

    # 3. Read from SQL Server via JDBC
    df = spark.read.jdbc(
        url        = JDBC_URL,
        table      = query,
        properties = JDBC_PROPS,
    )

    df = df.withColumn("ingestion_ts", F.lit(RUN_TS.isoformat()))
    
    row_count = df.count()
    print(f"  Rows fetched: {row_count:,}")

    if row_count == 0:
        print(f"  No new records — skipping write.")
        return

    # 4. Add pipeline metadata columns
    df = df.withColumn("pipeline_run_ts",  F.lit(RUN_TS.isoformat()))
    df = df.withColumn("pipeline_run_date", F.lit(PARTITION_DT))

    # 5. Lowercase all column names (consistency with Silver/Gold)
    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower())

    # 6. Add date partitions from watermark column
    df = df.withColumn("year",  F.year(F.col(watermark_col.lower())))
    df = df.withColumn("month", F.month(F.col(watermark_col.lower())))
    df = df.withColumn("day",   F.dayofmonth(F.col(watermark_col.lower())))

    # 7. Write to S3 Bronze — partitioned Parquet
    output_path = f"{BRONZE_PATH}/{table_name}"
    df.write \
      .mode("append") \
      .partitionBy("year", "month", "day") \
      .parquet(output_path)

    print(f"  Written to: {output_path}/year=YYYY/month=MM/day=DD/")

    # 8. Update watermark to max timestamp in this batch
    max_ts = df.agg(
        F.max(F.col(watermark_col.lower()))
    ).collect()[0][0]

    update_watermark(table_name, max_ts.isoformat())
    print(f"  Done ✅")

# ── Full load function (for static reference tables) ──────────────────────
def full_load(table_name):
    print(f"\n{'='*60}")
    print(f"Full load: {table_name}")

    df = spark.read.jdbc(
        url        = JDBC_URL,
        table      = f"dbo.{table_name}",
        properties = JDBC_PROPS,
    )

    df = df.withColumn("ingestion_ts", F.lit(RUN_TS.isoformat()))

    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower())

    df = df.withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat()))
    df = df.withColumn("pipeline_run_date", F.lit(PARTITION_DT))

    output_path = f"{BRONZE_PATH}/{table_name}"
    df.write \
      .mode("overwrite") \
      .parquet(output_path)

    print(f"  Written to: {output_path}/")
    print(f"  Rows: {df.count():,} ✅")

# ══════════════════════════════════════════════════════════════════════════
# MAIN — Run all three tables
# ══════════════════════════════════════════════════════════════════════════
incremental_load("order_items",        watermark_col="creation_time_utc")
full_load("order_item_options")
full_load("date_dim")

job.commit()
print("\n🎉 Bronze ingestion complete.")