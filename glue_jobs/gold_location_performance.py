"""
Gold Location Performance Job
This job ranks restaurants by revenue, order volume, and growth, identifying top and bottom performers.
"""

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

print(f"Gold Location Performance job started: {RUN_TS.isoformat()}")

# ══════════════════════════════════════════════════════════════════════════
# READ SILVER
# ══════════════════════════════════════════════════════════════════════════
silver_oi = glueContext.create_dynamic_frame.from_catalog(
    database   = SILVER_DB,
    table_name = "order_items",
).toDF()

print(f"order_items rows: {silver_oi.count():,}")

# ── Dataset date reference ─────────────────────────────────────────────────
max_order_date = silver_oi.agg(F.max("order_date")).collect()[0][0]
min_order_date = silver_oi.agg(F.min("order_date")).collect()[0][0]
print(f"Date range: {min_order_date} → {max_order_date}")

# ══════════════════════════════════════════════════════════════════════════
# 1. OVERALL RESTAURANT PERFORMANCE
# ══════════════════════════════════════════════════════════════════════════
restaurant_overall = silver_oi.groupBy("restaurant_id") \
    .agg(
        # Revenue
        F.sum("revenue").cast(DecimalType(14, 2))
         .alias("total_gross_revenue"),
        F.avg("revenue").cast(DecimalType(10, 2))
         .alias("avg_item_revenue"),

        # Orders
        F.countDistinct("order_id").alias("total_orders"),
        F.count("lineitem_id").alias("total_line_items"),

        # Customers
        F.countDistinct("user_id").alias("unique_customers"),
        F.countDistinct(
            F.when(F.col("is_loyalty") == True, F.col("user_id"))
        ).alias("loyalty_customers"),

        # Dates
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("order_date").alias("active_days"),

        # Categories
        F.countDistinct("item_category").alias("unique_categories"),
        F.countDistinct("item_name").alias("unique_items"),
    ) \
    .withColumn("avg_order_value",
        (F.col("total_gross_revenue") / F.col("total_orders"))
        .cast(DecimalType(10, 2))
    ) \
    .withColumn("avg_daily_revenue",
        (F.col("total_gross_revenue") / F.col("active_days"))
        .cast(DecimalType(10, 2))
    ) \
    .withColumn("loyalty_rate",
        (F.col("loyalty_customers") / F.col("unique_customers") * 100)
        .cast(DecimalType(5, 2))
    ) \
    .withColumn("items_per_order",
        (F.col("total_line_items") / F.col("total_orders"))
        .cast(DecimalType(5, 2))
    )

# ── Revenue ranking ────────────────────────────────────────────────────────
revenue_window = Window.orderBy(F.col("total_gross_revenue").desc())
orders_window  = Window.orderBy(F.col("total_orders").desc())
aov_window     = Window.orderBy(F.col("avg_order_value").desc())

restaurant_overall = restaurant_overall \
    .withColumn("revenue_rank",    F.rank().over(revenue_window)) \
    .withColumn("orders_rank",     F.rank().over(orders_window)) \
    .withColumn("aov_rank",        F.rank().over(aov_window)) \
    .withColumn("performance_tier",
        F.when(F.col("revenue_rank") <= 10,  F.lit("TOP_10"))
         .when(F.col("revenue_rank") <= 25,  F.lit("TOP_25"))
         .when(F.col("revenue_rank") <= 50,  F.lit("TOP_50"))
         .otherwise(F.lit("STANDARD"))
    ) \
    .withColumn("snapshot_date",     F.lit(SNAPSHOT_DT)) \
    .withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
    .withColumn("pipeline_run_date", F.lit(SNAPSHOT_DT))

total_restaurants = restaurant_overall.count()
print(f"Total restaurants: {total_restaurants:,}")

# ══════════════════════════════════════════════════════════════════════════
# 2. MONTHLY RESTAURANT PERFORMANCE (for trend/growth analysis)
# ══════════════════════════════════════════════════════════════════════════
monthly_perf = silver_oi.groupBy(
    "restaurant_id",
    F.year("order_date").alias("year"),
    F.month("order_date").alias("month"),
) \
.agg(
    F.sum("revenue").cast(DecimalType(14, 2)).alias("monthly_revenue"),
    F.countDistinct("order_id").alias("monthly_orders"),
    F.countDistinct("user_id").alias("monthly_unique_customers"),
    F.avg("revenue").cast(DecimalType(10, 2)).alias("avg_item_revenue"),
)

# ── Month-over-month revenue growth ───────────────────────────────────────
mom_window = Window \
    .partitionBy("restaurant_id") \
    .orderBy("year", "month")

monthly_perf = monthly_perf \
    .withColumn("prev_month_revenue",
        F.lag("monthly_revenue", 1).over(mom_window)) \
    .withColumn("mom_revenue_growth_pct",
        F.when(
            F.col("prev_month_revenue").isNotNull() &
            (F.col("prev_month_revenue") > 0),
            ((F.col("monthly_revenue") - F.col("prev_month_revenue"))
             / F.col("prev_month_revenue") * 100)
            .cast(DecimalType(8, 2))
        ).otherwise(F.lit(None))
    ) \
    .withColumn("snapshot_date",     F.lit(SNAPSHOT_DT)) \
    .withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
    .withColumn("pipeline_run_date", F.lit(SNAPSHOT_DT))

# ══════════════════════════════════════════════════════════════════════════
# 3. TOP ITEMS PER RESTAURANT
# ══════════════════════════════════════════════════════════════════════════
item_perf = silver_oi.groupBy("restaurant_id", "item_name", "item_category") \
    .agg(
        F.sum("revenue").cast(DecimalType(12, 2)).alias("item_revenue"),
        F.sum("item_quantity").alias("total_quantity_sold"),
        F.countDistinct("order_id").alias("orders_containing_item"),
    )

# Rank items within each restaurant
item_window = Window \
    .partitionBy("restaurant_id") \
    .orderBy(F.col("item_revenue").desc())

item_perf = item_perf \
    .withColumn("item_rank_in_restaurant",
        F.rank().over(item_window)) \
    .filter(F.col("item_rank_in_restaurant") <= 10) \
    .withColumn("snapshot_date",     F.lit(SNAPSHOT_DT)) \
    .withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
    .withColumn("pipeline_run_date", F.lit(SNAPSHOT_DT))

# ══════════════════════════════════════════════════════════════════════════
# LOG SUMMARY
# ══════════════════════════════════════════════════════════════════════════
print("\nTop 10 Restaurants by Revenue:")
restaurant_overall \
    .filter(F.col("revenue_rank") <= 10) \
    .select("restaurant_id", "total_gross_revenue", "total_orders",
            "unique_customers", "avg_order_value", "performance_tier") \
    .orderBy("revenue_rank") \
    .show(truncate=False)

print("\nBottom 10 Restaurants by Revenue:")
restaurant_overall \
    .orderBy(F.col("total_gross_revenue").asc()) \
    .select("restaurant_id", "total_gross_revenue",
            "total_orders", "unique_customers") \
    .show(10, truncate=False)

print(f"\nPerformance tier distribution:")
restaurant_overall.groupBy("performance_tier") \
    .count() \
    .orderBy("performance_tier") \
    .show()

# ══════════════════════════════════════════════════════════════════════════
# WRITE GOLD LOCATION PERFORMANCE
# ══════════════════════════════════════════════════════════════════════════
restaurant_overall.write \
    .mode("overwrite") \
    .partitionBy("snapshot_date") \
    .parquet(f"{GOLD_PATH}/location_performance/")

monthly_perf.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet(f"{GOLD_PATH}/location_monthly_performance/")

item_perf.write \
    .mode("overwrite") \
    .partitionBy("snapshot_date") \
    .parquet(f"{GOLD_PATH}/location_top_items/")

print(f"\n✅ Location performance written to Gold")
print(f"   Overall:  {GOLD_PATH}/location_performance/")
print(f"   Monthly:  {GOLD_PATH}/location_monthly_performance/")
print(f"   Items:    {GOLD_PATH}/location_top_items/")

job.commit()
print(f"\n🎉 Gold Location Performance complete: {datetime.now(timezone.utc).isoformat()}")