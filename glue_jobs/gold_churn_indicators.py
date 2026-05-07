"""
Gold Churn Indicators — Summary
Purpose: Build a customer activity profile to help marketing identify at-risk customers without any ML predictions — purely rule-based tagging based on observed behaviour.
"""

import sys
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DecimalType, IntegerType

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

print(f"Gold Churn Indicators job started: {RUN_TS.isoformat()}")

# ══════════════════════════════════════════════════════════════════════════
# READ SILVER — customer eligible rows only
# ══════════════════════════════════════════════════════════════════════════
silver_oi = glueContext.create_dynamic_frame.from_catalog(
    database   = SILVER_DB,
    table_name = "order_items",
).toDF().filter(F.col("eligible_for_customer_metrics") == True)

print(f"Customer-eligible rows: {silver_oi.count():,}")

# Use dataset max date as reference — not today
# Data spans 2020-2024, so all activity metrics must be relative
max_order_date = silver_oi.agg(F.max("order_date")).collect()[0][0]
print(f"Dataset max order date: {max_order_date}")

# ══════════════════════════════════════════════════════════════════════════
# STEP 1 — ORDER-LEVEL AGGREGATION PER CUSTOMER
# ══════════════════════════════════════════════════════════════════════════
# Get one row per customer per order date with daily spend
order_summary = silver_oi.groupBy("user_id", "order_date") \
    .agg(
        F.countDistinct("order_id").alias("orders_on_day"),
        F.sum("revenue").cast(DecimalType(12, 2)).alias("daily_spend"),
    )

# ══════════════════════════════════════════════════════════════════════════
# STEP 2 — COMPUTE ORDER GAPS PER CUSTOMER
# ══════════════════════════════════════════════════════════════════════════
# Use window function to get previous order date per customer
order_window = Window \
    .partitionBy("user_id") \
    .orderBy("order_date")

order_summary = order_summary \
    .withColumn("prev_order_date",
        F.lag("order_date", 1).over(order_window)) \
    .withColumn("days_gap",
        F.datediff(F.col("order_date"), F.col("prev_order_date")))

# ══════════════════════════════════════════════════════════════════════════
# STEP 3 — PERIOD SPEND FOR % CHANGE CALCULATION
# ══════════════════════════════════════════════════════════════════════════
# Split timeline into periods relative to dataset max date
# Period 1 = last 90 days of data, Period 2 = 91-180 days, Period 3 = 181-270 days

silver_oi_with_period = silver_oi \
    .withColumn("days_before_max",
        F.datediff(F.lit(str(max_order_date)), F.col("order_date"))) \
    .withColumn("period",
        F.when(F.col("days_before_max") <= 90,  F.lit("P1_last_90d"))
         .when(F.col("days_before_max") <= 180, F.lit("P2_91_180d"))
         .when(F.col("days_before_max") <= 270, F.lit("P3_181_270d"))
         .otherwise(F.lit("P4_older"))
    )

# Pivot to get spend per period per customer
period_spend = silver_oi_with_period \
    .groupBy("user_id") \
    .agg(
        F.sum(F.when(F.col("period") == "P1_last_90d",
            F.col("revenue")).otherwise(0))
         .cast(DecimalType(12, 2)).alias("spend_last_90d"),
        F.sum(F.when(F.col("period") == "P2_91_180d",
            F.col("revenue")).otherwise(0))
         .cast(DecimalType(12, 2)).alias("spend_91_180d"),
        F.sum(F.when(F.col("period") == "P3_181_270d",
            F.col("revenue")).otherwise(0))
         .cast(DecimalType(12, 2)).alias("spend_181_270d"),
    )

# ── % change: P1 vs P2 (most recent period vs previous) ───────────────────
period_spend = period_spend.withColumn("pct_spend_change_p1_vs_p2",
    F.when(
        F.col("spend_91_180d") > 0,
        (((F.col("spend_last_90d") - F.col("spend_91_180d"))
          / F.col("spend_91_180d")) * 100)
        .cast(DecimalType(8, 2))
    ).otherwise(F.lit(None))
)

# ══════════════════════════════════════════════════════════════════════════
# STEP 4 — CUSTOMER-LEVEL ACTIVITY PROFILE
# ══════════════════════════════════════════════════════════════════════════
activity_profile = silver_oi.groupBy("user_id") \
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("revenue").cast(DecimalType(14, 2)).alias("total_revenue"),
        F.max(F.col("is_loyalty").cast("int")).cast("boolean")
         .alias("is_loyalty_member"),
    ) \
    .withColumn("days_since_last_order",
        F.datediff(F.lit(str(max_order_date)), F.col("last_order_date"))) \
    .withColumn("customer_lifespan_days",
        F.datediff(F.col("last_order_date"), F.col("first_order_date")))

# ── Average gap between orders ─────────────────────────────────────────────
avg_gap = order_summary \
    .filter(F.col("days_gap").isNotNull()) \
    .groupBy("user_id") \
    .agg(
        F.avg("days_gap").cast(DecimalType(8, 1)).alias("avg_days_between_orders"),
        F.min("days_gap").cast(IntegerType()).alias("min_days_between_orders"),
        F.max("days_gap").cast(IntegerType()).alias("max_days_between_orders"),
        F.stddev("days_gap").cast(DecimalType(8, 1)).alias("stddev_days_between_orders"),
    )

# ══════════════════════════════════════════════════════════════════════════
# STEP 5 — JOIN ALL COMPONENTS
# ══════════════════════════════════════════════════════════════════════════
churn_df = activity_profile \
    .join(avg_gap,     on="user_id", how="left") \
    .join(period_spend, on="user_id", how="left")

# ══════════════════════════════════════════════════════════════════════════
# STEP 6 — CHURN RISK TAGS
# ══════════════════════════════════════════════════════════════════════════
# Tag based on days since last order relative to dataset max date
# and spending trend
churn_df = churn_df.withColumn("churn_risk_level",
    F.when(F.col("days_since_last_order") > 180,  F.lit("CRITICAL"))
     .when(F.col("days_since_last_order") > 90,   F.lit("HIGH"))
     .when(F.col("days_since_last_order") > 45,   F.lit("MEDIUM"))
     .otherwise(F.lit("LOW"))
)

# Spending trend tag
churn_df = churn_df.withColumn("spend_trend",
    F.when(F.col("pct_spend_change_p1_vs_p2").isNull(),
        F.lit("INSUFFICIENT_DATA"))
     .when(F.col("pct_spend_change_p1_vs_p2") >= 20,
        F.lit("GROWING"))
     .when(F.col("pct_spend_change_p1_vs_p2") >= -10,
        F.lit("STABLE"))
     .when(F.col("pct_spend_change_p1_vs_p2") >= -50,
        F.lit("DECLINING"))
     .otherwise(F.lit("SEVERELY_DECLINING"))
)

# Combined risk tag — highest risk = CRITICAL + DECLINING
churn_df = churn_df.withColumn("overall_risk_tag",
    F.when(
        (F.col("churn_risk_level") == "CRITICAL") &
        (F.col("spend_trend").isin("DECLINING", "SEVERELY_DECLINING")),
        F.lit("CHURNED")
    ).when(
        (F.col("churn_risk_level").isin("HIGH", "CRITICAL")) &
        (F.col("spend_trend") == "SEVERELY_DECLINING"),
        F.lit("CHURNED")
    ).when(
        F.col("churn_risk_level").isin("HIGH", "CRITICAL"),
        F.lit("AT_RISK")
    ).when(
        (F.col("churn_risk_level") == "MEDIUM") &
        (F.col("spend_trend").isin("DECLINING", "SEVERELY_DECLINING")),
        F.lit("AT_RISK")
    ).when(
        F.col("churn_risk_level") == "LOW",
        F.lit("ACTIVE")
    ).otherwise(F.lit("MONITOR"))
)

# ── Add metadata ───────────────────────────────────────────────────────────
churn_df = churn_df \
    .withColumn("reference_date",    F.lit(str(max_order_date))) \
    .withColumn("snapshot_date",     F.lit(SNAPSHOT_DT)) \
    .withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
    .withColumn("pipeline_run_date", F.lit(SNAPSHOT_DT))

# ══════════════════════════════════════════════════════════════════════════
# LOG SUMMARY
# ══════════════════════════════════════════════════════════════════════════
total = churn_df.count()
print(f"\nTotal customers profiled: {total:,}")

print("\nChurn Risk Level Distribution:")
churn_df.groupBy("churn_risk_level") \
    .agg(
        F.count("user_id").alias("customers"),
        F.round(F.avg("days_since_last_order"), 0).alias("avg_days_inactive"),
        F.round(F.avg("total_revenue"), 2).alias("avg_lifetime_revenue")
    ) \
    .orderBy("churn_risk_level") \
    .show()

print("\nOverall Risk Tag Distribution:")
churn_df.groupBy("overall_risk_tag") \
    .agg(
        F.count("user_id").alias("customers"),
        F.round(F.avg("total_revenue"), 2).alias("avg_revenue")
    ) \
    .orderBy(F.col("customers").desc()) \
    .show()

print("\nSpend Trend Distribution:")
churn_df.groupBy("spend_trend") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .show()

# ══════════════════════════════════════════════════════════════════════════
# WRITE GOLD CHURN INDICATORS
# ══════════════════════════════════════════════════════════════════════════
gold_churn = churn_df.select(
    "user_id",
    "first_order_date",
    "last_order_date",
    "days_since_last_order",
    "customer_lifespan_days",
    "total_orders",
    "total_revenue",
    "avg_days_between_orders",
    "min_days_between_orders",
    "max_days_between_orders",
    "stddev_days_between_orders",
    "spend_last_90d",
    "spend_91_180d",
    "spend_181_270d",
    "pct_spend_change_p1_vs_p2",
    "is_loyalty_member",
    "churn_risk_level",
    "spend_trend",
    "overall_risk_tag",
    "reference_date",
    "snapshot_date",
    "pipeline_run_ts",
    "pipeline_run_date",
)

gold_churn.write \
    .mode("overwrite") \
    .partitionBy("snapshot_date") \
    .parquet(f"{GOLD_PATH}/churn_indicators/")

print(f"\n✅ Churn indicators written: {total:,} customers")
print(f"   Path: {GOLD_PATH}/churn_indicators/snapshot_date={SNAPSHOT_DT}/")

job.commit()
print(f"\n🎉 Gold Churn Indicators complete: {datetime.now(timezone.utc).isoformat()}")