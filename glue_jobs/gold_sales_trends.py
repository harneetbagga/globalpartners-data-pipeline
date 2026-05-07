"""
Gold Sales Trends Job
This job computes daily/weekly/monthly revenue patterns by joining order_items with date_dim to enrich with calendar context (day of week, holidays, weekends).
"""

import sys
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# ── Init ───────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc   = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job   = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ── Config ─────────────────────────────────────────────────────────────────
SILVER_DB   = "gp_silver"
GOLD_PATH   = "s3://gp-data-lake-dev/gold"
RUN_TS      = datetime.now(timezone.utc)
SNAPSHOT_DT = RUN_TS.strftime("%Y-%m-%d")

print(f"Gold Sales Trends job started: {RUN_TS.isoformat()}")

# ══════════════════════════════════════════════════════════════════════════
# READ SILVER
# ══════════════════════════════════════════════════════════════════════════
silver_oi = glueContext.create_dynamic_frame.from_catalog(
    database   = SILVER_DB,
    table_name = "order_items",
).toDF()

silver_dd = glueContext.create_dynamic_frame.from_catalog(
    database   = SILVER_DB,
    table_name = "date_dim",
).toDF()

print(f"order_items rows: {silver_oi.count():,}")
print(f"date_dim rows:    {silver_dd.count():,}")

# ══════════════════════════════════════════════════════════════════════════
# JOIN ORDER_ITEMS WITH DATE_DIM
# ══════════════════════════════════════════════════════════════════════════
# Enrich orders with calendar attributes
enriched = silver_oi.join(
    silver_dd.select(
        "date_key",
        "day_of_week",
        "week",
        "is_weekend",
        "is_holiday",
        "holiday_name",
    ),
    silver_oi["order_date"] == silver_dd["date_key"],
    how = "left"
)

# ══════════════════════════════════════════════════════════════════════════
# 1. DAILY SALES TRENDS
# ══════════════════════════════════════════════════════════════════════════
daily_trends = enriched.groupBy(
    "order_date",
    "restaurant_id",
    "day_of_week",
    "is_weekend",
    "is_holiday",
    "holiday_name",
) \
.agg(
    F.countDistinct("order_id").alias("total_orders"),
    F.count("lineitem_id").alias("total_line_items"),
    F.sum("revenue").cast(DecimalType(14, 2)).alias("gross_revenue"),
    F.countDistinct("user_id").alias("unique_customers"),
    F.avg("revenue").cast(DecimalType(10, 2)).alias("avg_item_revenue"),
    F.sum(
        F.when(F.col("is_loyalty") == True, F.col("revenue"))
         .otherwise(0)
    ).cast(DecimalType(14, 2)).alias("loyalty_revenue"),
    F.sum(
        F.when(F.col("is_loyalty") == False, F.col("revenue"))
         .otherwise(0)
    ).cast(DecimalType(14, 2)).alias("non_loyalty_revenue"),
) \
.withColumn("avg_order_value",
    (F.col("gross_revenue") / F.col("total_orders"))
    .cast(DecimalType(10, 2))
) \
.withColumn("snapshot_date",     F.lit(SNAPSHOT_DT)) \
.withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
.withColumn("pipeline_run_date", F.lit(SNAPSHOT_DT)) \
.withColumn("year",  F.year("order_date")) \
.withColumn("month", F.month("order_date"))

print(f"Daily trend rows: {daily_trends.count():,}")

# ══════════════════════════════════════════════════════════════════════════
# 2. WEEKLY SALES TRENDS
# ══════════════════════════════════════════════════════════════════════════
weekly_trends = enriched.groupBy(
    F.year("order_date").alias("year"),
    "week",
    "restaurant_id",
) \
.agg(
    F.countDistinct("order_id").alias("total_orders"),
    F.sum("revenue").cast(DecimalType(14, 2)).alias("gross_revenue"),
    F.countDistinct("user_id").alias("unique_customers"),
    F.avg("revenue").cast(DecimalType(10, 2)).alias("avg_item_revenue"),
    F.min("order_date").alias("week_start_date"),
    F.max("order_date").alias("week_end_date"),
) \
.withColumn("avg_order_value",
    (F.col("gross_revenue") / F.col("total_orders"))
    .cast(DecimalType(10, 2))
) \
.withColumn("snapshot_date",     F.lit(SNAPSHOT_DT)) \
.withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
.withColumn("pipeline_run_date", F.lit(SNAPSHOT_DT))

print(f"Weekly trend rows: {weekly_trends.count():,}")

# ══════════════════════════════════════════════════════════════════════════
# 3. MONTHLY SALES TRENDS
# ══════════════════════════════════════════════════════════════════════════
monthly_trends = enriched.groupBy(
    F.year("order_date").alias("year"),
    F.month("order_date").alias("month"),
    "restaurant_id",
) \
.agg(
    F.countDistinct("order_id").alias("total_orders"),
    F.sum("revenue").cast(DecimalType(14, 2)).alias("gross_revenue"),
    F.countDistinct("user_id").alias("unique_customers"),
    F.avg("revenue").cast(DecimalType(10, 2)).alias("avg_item_revenue"),
    F.countDistinct(
        F.when(F.col("is_loyalty") == True, F.col("user_id"))
    ).alias("loyalty_customers"),
    F.countDistinct(
        F.when(F.col("is_loyalty") == False, F.col("user_id"))
    ).alias("non_loyalty_customers"),
    F.sum(
        F.when(F.col("is_holiday") == True, F.col("revenue"))
         .otherwise(0)
    ).cast(DecimalType(14, 2)).alias("holiday_revenue"),
) \
.withColumn("avg_order_value",
    (F.col("gross_revenue") / F.col("total_orders"))
    .cast(DecimalType(10, 2))
) \
.withColumn("snapshot_date",     F.lit(SNAPSHOT_DT)) \
.withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
.withColumn("pipeline_run_date", F.lit(SNAPSHOT_DT))

print(f"Monthly trend rows: {monthly_trends.count():,}")

# ══════════════════════════════════════════════════════════════════════════
# LOG SUMMARY
# ══════════════════════════════════════════════════════════════════════════
print("\nTop 5 revenue days:")
daily_trends.orderBy(F.col("gross_revenue").desc()) \
    .select("order_date", "restaurant_id", "gross_revenue",
            "total_orders", "is_holiday", "day_of_week") \
    .show(5, truncate=False)

print("\nMonthly revenue trend (all restaurants):")
monthly_trends.groupBy("year", "month") \
    .agg(F.sum("gross_revenue").cast(DecimalType(14,2)).alias("total_revenue")) \
    .orderBy("year", "month") \
    .show(20)

# ══════════════════════════════════════════════════════════════════════════
# WRITE GOLD SALES TRENDS
# ══════════════════════════════════════════════════════════════════════════
daily_trends.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"{GOLD_PATH}/sales_trends_daily/")

weekly_trends.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet(f"{GOLD_PATH}/sales_trends_weekly/")

monthly_trends.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet(f"{GOLD_PATH}/sales_trends_monthly/")

print(f"\n✅ Sales trends written to Gold")
print(f"   Daily:   {GOLD_PATH}/sales_trends_daily/")
print(f"   Weekly:  {GOLD_PATH}/sales_trends_weekly/")
print(f"   Monthly: {GOLD_PATH}/sales_trends_monthly/")

job.commit()
print(f"\n🎉 Gold Sales Trends complete: {datetime.now(timezone.utc).isoformat()}")
