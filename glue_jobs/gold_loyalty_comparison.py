import sys
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window
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

print(f"Gold Loyalty Comparison job started: {RUN_TS.isoformat()}")

# ══════════════════════════════════════════════════════════════════════════
# READ SILVER
# ══════════════════════════════════════════════════════════════════════════
silver_oi = glueContext.create_dynamic_frame.from_catalog(
    database   = SILVER_DB,
    table_name = "order_items",
).toDF()

silver_oio = glueContext.create_dynamic_frame.from_catalog(
    database   = SILVER_DB,
    table_name = "order_item_options",
).toDF()

print(f"order_items rows: {silver_oi.count():,}")

# Use dataset max date as reference
max_order_date = silver_oi.agg(F.max("order_date")).collect()[0][0]
print(f"Dataset max order date: {max_order_date}")

# ══════════════════════════════════════════════════════════════════════════
# JOIN WITH OPTIONS TO GET NET REVENUE
# ══════════════════════════════════════════════════════════════════════════
option_totals = silver_oio.groupBy("order_id", "lineitem_id") \
    .agg(
        F.sum("option_price").cast(DecimalType(12, 2))
         .alias("total_option_price"),
        F.sum(F.when(F.col("is_discount") == True, F.col("option_price"))
              .otherwise(0)).cast(DecimalType(12, 2))
         .alias("total_discount"),
    )

enriched = silver_oi.join(
    option_totals,
    on  = ["order_id", "lineitem_id"],
    how = "left"
).fillna({"total_option_price": 0, "total_discount": 0}) \
 .withColumn("net_revenue",
    (F.col("revenue") + F.col("total_option_price"))
    .cast(DecimalType(12, 2))
 )

# ══════════════════════════════════════════════════════════════════════════
# 1. CUSTOMER-LEVEL LOYALTY COMPARISON
# ══════════════════════════════════════════════════════════════════════════
# For eligible customers only — need user_id for customer metrics
customer_enriched = enriched.filter(
    F.col("eligible_for_customer_metrics") == True
)

customer_profile = customer_enriched.groupBy("user_id", "is_loyalty") \
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("net_revenue").cast(DecimalType(14, 2)).alias("total_net_revenue"),
        F.avg("net_revenue").cast(DecimalType(10, 2)).alias("avg_item_revenue"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("restaurant_id").alias("unique_restaurants"),
        F.countDistinct("item_category").alias("unique_categories"),
    ) \
    .withColumn("days_as_customer",
        F.datediff(F.col("last_order_date"), F.col("first_order_date"))) \
    .withColumn("days_since_last_order",
        F.datediff(F.lit(str(max_order_date)), F.col("last_order_date"))) \
    .withColumn("avg_order_value",
        (F.col("total_net_revenue") / F.col("total_orders"))
        .cast(DecimalType(10, 2))
    )

# ══════════════════════════════════════════════════════════════════════════
# 2. AGGREGATE LOYALTY vs NON-LOYALTY SUMMARY
# ══════════════════════════════════════════════════════════════════════════
loyalty_summary = customer_profile.groupBy("is_loyalty") \
    .agg(
        F.count("user_id").alias("total_customers"),
        F.round(F.avg("total_net_revenue"), 2).alias("avg_clv"),
        F.round(F.avg("total_orders"), 1).alias("avg_orders"),
        F.round(F.avg("avg_order_value"), 2).alias("avg_order_value"),
        F.round(F.avg("days_as_customer"), 0).alias("avg_customer_lifespan_days"),
        F.round(F.avg("days_since_last_order"), 0).alias("avg_days_since_last_order"),
        F.round(F.avg("unique_restaurants"), 1).alias("avg_restaurants_visited"),
        F.round(F.avg("unique_categories"), 1).alias("avg_categories_purchased"),
        F.round(F.sum("total_net_revenue"), 2).alias("total_revenue_contribution"),
        F.percentile_approx("total_net_revenue", 0.5).alias("median_clv"),
        F.percentile_approx("total_orders", 0.5).alias("median_orders"),
    ) \
    .withColumn("loyalty_label",
        F.when(F.col("is_loyalty") == True, F.lit("Loyalty Member"))
         .otherwise(F.lit("Non-Loyalty"))
    ) \
    .withColumn("snapshot_date",     F.lit(SNAPSHOT_DT)) \
    .withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
    .withColumn("pipeline_run_date", F.lit(SNAPSHOT_DT))

# ══════════════════════════════════════════════════════════════════════════
# 3. REVENUE BY ITEM CATEGORY — LOYALTY vs NON-LOYALTY
# ══════════════════════════════════════════════════════════════════════════
category_loyalty = enriched \
    .filter(F.col("item_category").isNotNull()) \
    .groupBy("item_category", "is_loyalty") \
    .agg(
        F.sum("net_revenue").cast(DecimalType(14, 2)).alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("user_id").alias("unique_customers"),
        F.round(F.avg("net_revenue"), 2).alias("avg_item_revenue"),
    ) \
    .withColumn("loyalty_label",
        F.when(F.col("is_loyalty") == True, F.lit("Loyalty Member"))
         .otherwise(F.lit("Non-Loyalty"))
    ) \
    .withColumn("snapshot_date",     F.lit(SNAPSHOT_DT)) \
    .withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
    .withColumn("pipeline_run_date", F.lit(SNAPSHOT_DT))

# ══════════════════════════════════════════════════════════════════════════
# 4. MONTHLY LOYALTY TREND
# ══════════════════════════════════════════════════════════════════════════
monthly_loyalty = enriched.groupBy(
    F.year("order_date").alias("year"),
    F.month("order_date").alias("month"),
    "is_loyalty"
) \
.agg(
    F.countDistinct("order_id").alias("total_orders"),
    F.sum("net_revenue").cast(DecimalType(14, 2)).alias("total_revenue"),
    F.countDistinct("user_id").alias("unique_customers"),
) \
.withColumn("loyalty_label",
    F.when(F.col("is_loyalty") == True, F.lit("Loyalty Member"))
     .otherwise(F.lit("Non-Loyalty"))
) \
.withColumn("snapshot_date",     F.lit(SNAPSHOT_DT)) \
.withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
.withColumn("pipeline_run_date", F.lit(SNAPSHOT_DT))

# ══════════════════════════════════════════════════════════════════════════
# LOG SUMMARY
# ══════════════════════════════════════════════════════════════════════════
print("\nLoyalty vs Non-Loyalty Summary:")
loyalty_summary.select(
    "loyalty_label", "total_customers", "avg_clv",
    "avg_orders", "avg_order_value", "avg_days_since_last_order",
    "total_revenue_contribution"
).show(truncate=False)

print("\nTop 5 Categories by Revenue — Loyalty vs Non-Loyalty:")
category_loyalty.orderBy(F.col("total_revenue").desc()) \
    .select("item_category", "loyalty_label", "total_revenue", "total_orders") \
    .show(10, truncate=False)

# ══════════════════════════════════════════════════════════════════════════
# WRITE GOLD LOYALTY COMPARISON
# ══════════════════════════════════════════════════════════════════════════
loyalty_summary.write \
    .mode("overwrite") \
    .partitionBy("snapshot_date") \
    .parquet(f"{GOLD_PATH}/loyalty_summary/")

category_loyalty.write \
    .mode("overwrite") \
    .partitionBy("snapshot_date") \
    .parquet(f"{GOLD_PATH}/loyalty_by_category/")

monthly_loyalty.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet(f"{GOLD_PATH}/loyalty_monthly_trend/")

print(f"\n✅ Loyalty comparison written to Gold")
print(f"   Summary:        {GOLD_PATH}/loyalty_summary/")
print(f"   By category:    {GOLD_PATH}/loyalty_by_category/")
print(f"   Monthly trend:  {GOLD_PATH}/loyalty_monthly_trend/")

job.commit()
print(f"\n🎉 Gold Loyalty Comparison complete: {datetime.now(timezone.utc).isoformat()}")