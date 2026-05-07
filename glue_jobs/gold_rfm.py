"""
Gold RFM Job
RFM segments customers by Recency (how recently they ordered), Frequency (how often), and Monetary (how much they spent). Each dimension is scored 1-5, then combined into a segment label.
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

print(f"Gold RFM job started: {RUN_TS.isoformat()}")

# ══════════════════════════════════════════════════════════════════════════
# READ SILVER — customer eligible rows only
# ══════════════════════════════════════════════════════════════════════════
silver_oi = glueContext.create_dynamic_frame.from_catalog(
    database   = SILVER_DB,
    table_name = "order_items",
).toDF().filter(F.col("eligible_for_customer_metrics") == True)

print(f"Customer-eligible rows: {silver_oi.count():,}")

# ── Use dataset max date as reference (not today) ──────────────────────────
# Data spans 2020-2024, so churn/recency must be relative to dataset cutoff
max_order_date = silver_oi.agg(F.max("order_date")).collect()[0][0]
print(f"Dataset max order date: {max_order_date}")

# ══════════════════════════════════════════════════════════════════════════
# COMPUTE RFM BASE METRICS PER CUSTOMER
# ══════════════════════════════════════════════════════════════════════════
rfm_base = silver_oi.groupBy("user_id").agg(

    # Recency — days since last order relative to dataset max date
    F.datediff(
        F.lit(str(max_order_date)),
        F.max("order_date")
    ).alias("recency_days"),

    # Frequency — distinct orders in full dataset
    F.countDistinct("order_id").alias("frequency"),

    # Monetary — total net revenue
    F.sum("revenue").cast(DecimalType(14, 2)).alias("monetary"),

    # Supporting metrics
    F.min("order_date").alias("first_order_date"),
    F.max("order_date").alias("last_order_date"),
    F.countDistinct("order_date").alias("active_days"),
    F.max(F.col("is_loyalty").cast("int")).cast("boolean")
     .alias("is_loyalty_member"),
)

print(f"Unique customers for RFM: {rfm_base.count():,}")

# ══════════════════════════════════════════════════════════════════════════
# SCORE EACH DIMENSION INTO QUINTILES (1-5)
# ══════════════════════════════════════════════════════════════════════════
# RECENCY:  lower days = more recent = higher score (5 is best)
# FREQUENCY: higher count = higher score (5 is best)
# MONETARY:  higher spend = higher score (5 is best)

# Use ntile(5) for equal-sized buckets
recency_window   = Window.orderBy(F.col("recency_days").desc())   # desc → recent = high score
frequency_window = Window.orderBy(F.col("frequency").asc())        # asc  → high freq = high score
monetary_window  = Window.orderBy(F.col("monetary").asc())         # asc  → high spend = high score

rfm_scored = rfm_base \
    .withColumn("r_score", F.ntile(5).over(recency_window)) \
    .withColumn("f_score", F.ntile(5).over(frequency_window)) \
    .withColumn("m_score", F.ntile(5).over(monetary_window))

# ── Combined RFM score ─────────────────────────────────────────────────────
rfm_scored = rfm_scored.withColumn("rfm_score",
    F.col("r_score") + F.col("f_score") + F.col("m_score")
)

# ══════════════════════════════════════════════════════════════════════════
# SEGMENT ASSIGNMENT
# ══════════════════════════════════════════════════════════════════════════
# Segments based on R and F scores (Monetary is secondary)
rfm_scored = rfm_scored.withColumn("segment",
    F.when(
        (F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4),
        F.lit("VIP")                          # Recent + frequent = best customers
    ).when(
        (F.col("r_score") >= 3) & (F.col("f_score") >= 3),
        F.lit("Loyal Customers")              # Consistently engaged
    ).when(
        (F.col("r_score") >= 4) & (F.col("f_score") <= 2),
        F.lit("New Customers")                # Recent but low frequency
    ).when(
        (F.col("r_score") >= 3) & (F.col("f_score") <= 2),
        F.lit("Promising")                    # Somewhat recent, low frequency
    ).when(
        (F.col("r_score") == 3) & (F.col("f_score") == 3),
        F.lit("Need Attention")               # Average on both — at risk
    ).when(
        (F.col("r_score") <= 2) & (F.col("f_score") >= 4),
        F.lit("At Risk")                      # Used to buy often, now inactive
    ).when(
        (F.col("r_score") <= 2) & (F.col("f_score") >= 2),
        F.lit("Hibernating")                  # Below average recency and frequency
    ).when(
        (F.col("r_score") == 1) & (F.col("f_score") == 1),
        F.lit("Lost")                         # Lowest recency and frequency
    ).otherwise(F.lit("Others"))
)

# ── Add churn risk flag (relative to dataset max date) ────────────────────
rfm_scored = rfm_scored.withColumn("is_churn_risk",
    F.when(F.col("recency_days") > 45, F.lit(True))
     .otherwise(F.lit(False))
)

# ── Add metadata ───────────────────────────────────────────────────────────
rfm_scored = rfm_scored \
    .withColumn("reference_date",    F.lit(str(max_order_date))) \
    .withColumn("snapshot_date",     F.lit(SNAPSHOT_DT)) \
    .withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
    .withColumn("pipeline_run_date", F.lit(SNAPSHOT_DT))

# ══════════════════════════════════════════════════════════════════════════
# LOG SUMMARY
# ══════════════════════════════════════════════════════════════════════════
print("\nRFM Segment Distribution:")
rfm_scored.groupBy("segment") \
    .agg(
        F.count("user_id").alias("customers"),
        F.round(F.avg("monetary"), 2).alias("avg_spend"),
        F.round(F.avg("frequency"), 1).alias("avg_orders"),
        F.round(F.avg("recency_days"), 0).alias("avg_days_since_order")
    ) \
    .orderBy(F.col("customers").desc()) \
    .show(truncate=False)

print("\nRFM Score Distribution:")
rfm_scored.groupBy("r_score", "f_score") \
    .count() \
    .orderBy("r_score", "f_score") \
    .show()

# ══════════════════════════════════════════════════════════════════════════
# WRITE GOLD RFM
# ══════════════════════════════════════════════════════════════════════════
gold_rfm = rfm_scored.select(
    "user_id",
    "recency_days",
    "frequency",
    "monetary",
    "first_order_date",
    "last_order_date",
    "active_days",
    "is_loyalty_member",
    "r_score",
    "f_score",
    "m_score",
    "rfm_score",
    "segment",
    "is_churn_risk",
    "reference_date",
    "snapshot_date",
    "pipeline_run_ts",
    "pipeline_run_date",
)

gold_rfm.write \
    .mode("overwrite") \
    .partitionBy("snapshot_date") \
    .parquet(f"{GOLD_PATH}/rfm/")

total = gold_rfm.count()
print(f"\n✅ Gold RFM written: {total:,} customers")
print(f"   Path: {GOLD_PATH}/rfm/snapshot_date={SNAPSHOT_DT}/")

job.commit()
print(f"\n🎉 Gold RFM complete: {datetime.now(timezone.utc).isoformat()}")