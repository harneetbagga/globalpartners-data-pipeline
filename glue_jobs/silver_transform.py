import sys
import boto3
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    DecimalType, IntegerType, BooleanType
)
# ── Init ───────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc   = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job   = Job(glueContext)
job.init(args["JOB_NAME"], args)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# ── Config ─────────────────────────────────────────────────────────────────
BRONZE_DB    = "gp_bronze"
SILVER_PATH  = "s3://gp-data-lake-dev/silver"
DLQ_PATH     = "s3://gp-data-lake-dev/dlq"
RUN_TS       = datetime.now(timezone.utc)
PARTITION_DT = RUN_TS.strftime("%Y-%m-%d")
print(f"Silver transform started: {RUN_TS.isoformat()}")
print(f"Run date: {PARTITION_DT}")
# ══════════════════════════════════════════════════════════════════════════
# HELPER — Log data quality summary
# ══════════════════════════════════════════════════════════════════════════
def log_dq_summary(df, table_name):
    """Print null counts and DQ flag distribution for visibility."""
    print(f"\n  --- DQ Summary: {table_name} ---")
    total = df.count()
    print(f"  Total rows: {total:,}")
    # Null counts per column
    null_counts = df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df.columns
        if c not in ["pipeline_run_ts", "pipeline_run_date", "silver_loaded_at"]
    ])
    null_row = null_counts.collect()[0].asDict()
    non_zero_nulls = {k: v for k, v in null_row.items() if v > 0}
    if non_zero_nulls:
        print(f"  Null counts: {non_zero_nulls}")
    else:
        print(f"  No nulls found")
    # DQ flag distribution if column exists
    if "dq_flag" in df.columns:
        print(f"  DQ flag distribution:")
        df.groupBy("dq_flag").count().orderBy("count", ascending=False).show()
    return total
# ══════════════════════════════════════════════════════════════════════════
# 1. SILVER ORDER_ITEMS
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Processing: order_items")
# ── Read from Bronze catalog ───────────────────────────────────────────────
bronze_oi = glueContext.create_dynamic_frame.from_catalog(
    database    = BRONZE_DB,
    table_name  = "order_items"
).toDF()
print(f"  Bronze rows read: {bronze_oi.count():,}")
# ── Step 1: Cast all columns to correct types ──────────────────────────────
bronze_oi = bronze_oi \
    .withColumn("creation_time_utc",
        F.to_timestamp(F.col("creation_time_utc"))) \
    .withColumn("item_price",
        F.col("item_price").cast(DecimalType(10, 2))) \
    .withColumn("item_quantity",
        F.col("item_quantity").cast(IntegerType())) \
    .withColumn("is_loyalty",
        F.col("is_loyalty").cast(BooleanType())) \
    .withColumn("order_date",
        F.to_date(F.col("creation_time_utc"))) \
    .withColumn("revenue",
        (F.col("item_price") * F.col("item_quantity"))
        .cast(DecimalType(12, 2)))
# ── Step 2: DQ Flagging (based on your business rules) ────────────────────
# HARD DROP: lineitem_id is null → only truly critical key
# KEEP WITH FLAG: user_id null (~17K) → useful for non-customer analytics
# KEEP WITH FLAG: printed_card_number null (~157K) → non-critical loyalty field
# KEEP WITH FLAG: item_price null → flag but keep for location/item analysis
# KEEP WITH FLAG: negative item_price → flag (could be a correction)
bronze_oi = bronze_oi.withColumn("dq_flag",
    F.when(F.col("lineitem_id").isNull(),
        F.lit("NULL_LINEITEM_ID"))                          # ← HARD DROP
     .when(F.col("order_id").isNull(),
        F.lit("NULL_ORDER_ID"))                             # ← HARD DROP
     .when(F.col("user_id").isNull(),
        F.lit("NULL_USER_ID"))                              # ← KEEP, flag only
     .when(F.col("printed_card_number").isNull(),
        F.lit("NULL_CARD_NUMBER"))                          # ← KEEP, flag only
     .when(F.col("item_price").isNull(),
        F.lit("NULL_PRICE"))                                # ← KEEP, flag only
     .when(F.col("item_price") < 0,
        F.lit("NEGATIVE_PRICE"))                            # ← KEEP, flag only
     .when(F.col("item_quantity") <= 0,
        F.lit("INVALID_QUANTITY"))                          # ← KEEP, flag only
     .when(F.col("creation_time_utc").isNull(),
        F.lit("NULL_TIMESTAMP"))                            # ← KEEP, flag only
     .otherwise(F.lit("CLEAN"))
)
# ── Step 3: Hard drop only truly critical failures ─────────────────────────
# Only drop when lineitem_id or order_id is null — everything else stays
hard_drop = bronze_oi.filter(
    F.col("dq_flag").isin(["NULL_LINEITEM_ID", "NULL_ORDER_ID"])
)
silver_oi = bronze_oi.filter(
    ~F.col("dq_flag").isin(["NULL_LINEITEM_ID", "NULL_ORDER_ID"])
)
drop_count = hard_drop.count()
print(f"  Hard dropped rows (null lineitem_id/order_id): {drop_count:,}")
# Write hard drops to DLQ for traceability
if drop_count > 0:
    hard_drop.write \
        .mode("append") \
        .parquet(f"{DLQ_PATH}/order_items/run_date={PARTITION_DT}/")
    print(f"  DLQ written for {drop_count:,} critical null rows")
# ── Step 4: Deduplicate using lineitem_id as PK ────────────────────────────
# lineitem_id is treated as primary key per your analysis
# Keep latest record per lineitem_id based on ingestion_ts
dedup_window = Window \
    .partitionBy("lineitem_id") \
    .orderBy(F.col("ingestion_ts").desc())
silver_oi = silver_oi \
    .withColumn("row_num", F.row_number().over(dedup_window)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")
print(f"  Rows after dedup: {silver_oi.count():,}")
# ── Step 5: Add customer analytics eligibility flag ───────────────────────
# This flag tells downstream Gold jobs whether the row can be used
# for customer-level metrics (CLV, RFM, churn)
# Rows with null user_id are kept but excluded from customer metrics
silver_oi = silver_oi.withColumn("eligible_for_customer_metrics",
    F.when(
        F.col("user_id").isNotNull(),
        F.lit(True)
    ).otherwise(F.lit(False))
)
# ── Step 6: Add pipeline metadata ─────────────────────────────────────────
silver_oi = silver_oi \
    .withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
    .withColumn("pipeline_run_date", F.lit(PARTITION_DT)) \
    .withColumn("silver_loaded_at",  F.current_timestamp())
# ── Step 7: Log DQ summary ────────────────────────────────────────────────
log_dq_summary(silver_oi, "order_items")
# ── Step 8: Select final Silver columns ───────────────────────────────────
silver_oi_final = silver_oi.select(
    # Business keys
    "order_id",
    "lineitem_id",              # PK per analysis
    # Customer identifiers (may be null — kept intentionally)
    "user_id",                  # null ~17K — kept for non-customer analytics
    "printed_card_number",      # null ~157K — kept, non-critical loyalty field
    # Restaurant / app context
    "restaurant_id",
    "app_name",
    # Item details
    "item_name",
    "item_category",
    "item_price",
    "item_quantity",
    "revenue",
    "currency",
    # Loyalty
    "is_loyalty",
    # Timestamps
    "creation_time_utc",
    "order_date",
    # Data quality
    "dq_flag",
    "eligible_for_customer_metrics",
    # Pipeline metadata
    "pipeline_run_ts",
    "pipeline_run_date",
    "silver_loaded_at",
    # Partition columns — always last
    F.year("order_date").alias("year"),
    F.month("order_date").alias("month"),
    F.dayofmonth("order_date").alias("day"),
)
# ── Step 9: Write Silver order_items ──────────────────────────────────────
silver_oi_final.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(f"{SILVER_PATH}/order_items/")
print(f"  ✅ order_items written to Silver")
# ══════════════════════════════════════════════════════════════════════════
# 2. SILVER ORDER_ITEM_OPTIONS
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Processing: order_item_options")
bronze_oio = glueContext.create_dynamic_frame.from_catalog(
    database   = BRONZE_DB,
    table_name = "order_item_options",
).toDF()
print(f"  Bronze rows read: {bronze_oio.count():,}")
# ── Cast types ────────────────────────────────────────────────────────────
bronze_oio = bronze_oio \
    .withColumn("option_price",
        F.col("option_price").cast(DecimalType(10, 2))) \
    .withColumn("option_quantity",
        F.col("option_quantity").cast(IntegerType())) \
    .withColumn("is_discount",
        F.when(F.col("option_price") < 0, F.lit(True))
         .otherwise(F.lit(False)))
# ── DQ Flagging ───────────────────────────────────────────────────────────
# Composite PK: (lineitem_id, option_group_name, option_name)
bronze_oio = bronze_oio.withColumn("dq_flag",
    F.when(F.col("order_id").isNull(),
        F.lit("NULL_ORDER_ID"))
     .when(F.col("lineitem_id").isNull(),
        F.lit("NULL_LINEITEM_ID"))
     .when(F.col("option_name").isNull(),
        F.lit("NULL_OPTION_NAME"))
     .when(F.col("option_group_name").isNull(),
        F.lit("NULL_OPTION_GROUP"))                         # ← flag, don't drop
     .when(F.col("option_price").isNull(),
        F.lit("NULL_OPTION_PRICE"))                         # ← flag, don't drop
     .otherwise(F.lit("CLEAN"))
)
# Hard drop only when composite PK is broken
hard_drop_oio = bronze_oio.filter(
    F.col("dq_flag").isin([
        "NULL_ORDER_ID",
        "NULL_LINEITEM_ID",
        "NULL_OPTION_NAME"
    ])
)
silver_oio = bronze_oio.filter(
    ~F.col("dq_flag").isin([
        "NULL_ORDER_ID",
        "NULL_LINEITEM_ID",
        "NULL_OPTION_NAME"
    ])
)
drop_count_oio = hard_drop_oio.count()
if drop_count_oio > 0:
    hard_drop_oio.write \
        .mode("append") \
        .parquet(f"{DLQ_PATH}/order_item_options/run_date={PARTITION_DT}/")
# ── Deduplicate on composite PK ───────────────────────────────────────────
# (lineitem_id, option_group_name, option_name) per your analysis
dedup_window_oio = Window \
    .partitionBy("lineitem_id", "option_group_name", "option_name") \
    .orderBy(F.col("ingestion_ts").desc())
silver_oio = silver_oio \
    .withColumn("row_num", F.row_number().over(dedup_window_oio)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")
print(f"  Clean rows after dedup: {silver_oio.count():,}")
# ── Add metadata and write ─────────────────────────────────────────────────
silver_oio = silver_oio \
    .withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
    .withColumn("pipeline_run_date", F.lit(PARTITION_DT)) \
    .withColumn("silver_loaded_at",  F.current_timestamp())
log_dq_summary(silver_oio, "order_item_options")
silver_oio_final = silver_oio.select(
    # Composite PK
    "order_id",
    "lineitem_id",
    "option_group_name",
    "option_name",
    # Metrics
    "option_price",
    "option_quantity",
    "is_discount",
    # DQ
    "dq_flag",
    # Pipeline metadata
    "pipeline_run_ts",
    "pipeline_run_date",
    "silver_loaded_at",
)
silver_oio_final.write \
    .mode("overwrite") \
    .parquet(f"{SILVER_PATH}/order_item_options/")
print(f"  ✅ order_item_options written to Silver")
# ══════════════════════════════════════════════════════════════════════════
# 3. SILVER DATE_DIM
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Processing: date_dim")
bronze_dd = glueContext.create_dynamic_frame.from_catalog(
    database   = BRONZE_DB,
    table_name = "date_dim",
).toDF()
silver_dd = bronze_dd \
    .withColumn("date_key",
        F.to_date(F.col("date_key"))) \
    .withColumn("year",
        F.col("year").cast(IntegerType())) \
    .withColumn("month",
        F.col("month").cast(IntegerType())) \
    .withColumn("week",
        F.col("week").cast(IntegerType())) \
    .withColumn("is_weekend",
        F.col("is_weekend").cast(BooleanType())) \
    .withColumn("is_holiday",
        F.col("is_holiday").cast(BooleanType())) \
    .withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
    .withColumn("pipeline_run_date", F.lit(PARTITION_DT)) \
    .withColumn("silver_loaded_at",  F.current_timestamp())
silver_dd.coalesce(1).write \
    .mode("overwrite") \
    .parquet(f"{SILVER_PATH}/date_dim/")
print(f"  ✅ date_dim written to Silver: {silver_dd.count():,} rows")
# ══════════════════════════════════════════════════════════════════════════
# DONE
# ══════════════════════════════════════════════════════════════════════════
job.commit()
print(f"\n🎉 Silver transform complete: {datetime.now(timezone.utc).isoformat()}")