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

print(f"Gold CLV job started: {RUN_TS.isoformat()}")
print(f"Snapshot date: {SNAPSHOT_DT}")

# ══════════════════════════════════════════════════════════════════════════
# READ SILVER ORDER_ITEMS
# ══════════════════════════════════════════════════════════════════════════
silver_oi = glueContext.create_dynamic_frame.from_catalog(
    database   = SILVER_DB,
    table_name = "order_items",
).toDF()

print(f"Silver rows read: {silver_oi.count():,}")

# ── Filter to customer-eligible rows only ──────────────────────────────────
# Rows with null user_id are kept in Silver for location/item analysis
# but excluded here since CLV requires a customer identifier
customer_oi = silver_oi.filter(
    F.col("eligible_for_customer_metrics") == True
)

print(f"Customer-eligible rows: {customer_oi.count():,}")

# ── Join with order_item_options to get net revenue ────────────────────────
# option_price can be negative (discounts) so net revenue
# = item revenue + option adjustments
silver_oio = glueContext.create_dynamic_frame.from_catalog(
    database   = SILVER_DB,
    table_name = "order_item_options",
).toDF()

# Aggregate option prices per lineitem
option_totals = silver_oio.groupBy("order_id", "lineitem_id") \
    .agg(
        F.sum("option_price").alias("total_option_price"),
        F.sum(
            F.when(F.col("option_price") < 0, F.col("option_price"))
             .otherwise(0)
        ).alias("total_discount_amount"),
        F.sum(
            F.when(F.col("is_discount") == True, 1).otherwise(0)
        ).alias("discount_item_count")
    )

# Join options onto order items
customer_oi = customer_oi.join(
    option_totals,
    on  = ["order_id", "lineitem_id"],
    how = "left"
).fillna({"total_option_price": 0, "total_discount_amount": 0, "discount_item_count": 0})

# Compute net revenue per lineitem
customer_oi = customer_oi.withColumn("net_revenue",
    (F.col("revenue") + F.col("total_option_price"))
    .cast(DecimalType(12, 2))
)

# ══════════════════════════════════════════════════════════════════════════
# COMPUTE CLV PER CUSTOMER
# ══════════════════════════════════════════════════════════════════════════
clv_df = customer_oi.groupBy("user_id") \
    .agg(
        # Revenue metrics
        F.sum("revenue").cast(DecimalType(14, 2))
         .alias("total_gross_revenue"),
        F.sum("net_revenue").cast(DecimalType(14, 2))
         .alias("total_net_revenue"),
        F.sum("total_discount_amount").cast(DecimalType(14, 2))
         .alias("total_discounts_received"),

        # Order metrics
        F.countDistinct("order_id")
         .alias("total_orders"),
        F.count("lineitem_id")
         .alias("total_line_items"),
        F.avg("net_revenue").cast(DecimalType(10, 2))
         .alias("avg_order_item_value"),

        # Time metrics
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("order_date").alias("active_days"),

        # Loyalty
        F.max(F.col("is_loyalty").cast("int")).cast("boolean")
         .alias("is_loyalty_member"),
        F.countDistinct(
            F.when(F.col("is_loyalty") == True, F.col("order_id"))
        ).alias("loyalty_orders"),

        # Location
        F.countDistinct("restaurant_id").alias("unique_restaurants_visited"),
    )

# ── Days since first and last order ───────────────────────────────────────
clv_df = clv_df \
    .withColumn("days_as_customer",
        F.datediff(F.lit(SNAPSHOT_DT), F.col("first_order_date"))) \
    .withColumn("days_since_last_order",
        F.datediff(F.lit(SNAPSHOT_DT), F.col("last_order_date"))) \
    .withColumn("avg_order_frequency_days",
        F.when(F.col("total_orders") > 1,
            (F.col("days_as_customer") / (F.col("total_orders") - 1))
            .cast(DecimalType(8, 1))
        ).otherwise(F.lit(None))
    )

# ── CLV Band Assignment using PERCENT_RANK ─────────────────────────────────
# HIGH   = top 20% by total net revenue
# MEDIUM = middle 60%
# LOW    = bottom 20%
revenue_window = Window.orderBy(F.col("total_net_revenue"))

clv_df = clv_df.withColumn("revenue_percentile",
    F.percent_rank().over(revenue_window)
)

clv_df = clv_df.withColumn("clv_band",
    F.when(F.col("revenue_percentile") >= 0.80, F.lit("HIGH"))
     .when(F.col("revenue_percentile") >= 0.20, F.lit("MEDIUM"))
     .otherwise(F.lit("LOW"))
)

# ── Churn risk flag ────────────────────────────────────────────────────────
# Customers inactive for more than 45 days are flagged as churn risk
clv_df = clv_df.withColumn("is_churn_risk",
    F.when(F.col("days_since_last_order") > 45, F.lit(True))
     .otherwise(F.lit(False))
)

# ── Add snapshot metadata ──────────────────────────────────────────────────
clv_df = clv_df \
    .withColumn("snapshot_date",      F.lit(SNAPSHOT_DT)) \
    .withColumn("pipeline_run_ts",    F.lit(RUN_TS.isoformat())) \
    .withColumn("pipeline_run_date",  F.lit(SNAPSHOT_DT))

# ══════════════════════════════════════════════════════════════════════════
# LOG SUMMARY
# ══════════════════════════════════════════════════════════════════════════
total_customers = clv_df.count()
print(f"\nTotal unique customers: {total_customers:,}")

# CLV band distribution
print("\nCLV Band Distribution:")
clv_df.groupBy("clv_band") \
    .agg(
        F.count("user_id").alias("customers"),
        F.avg("total_net_revenue").cast(DecimalType(10,2)).alias("avg_net_revenue"),
        F.min("total_net_revenue").alias("min_revenue"),
        F.max("total_net_revenue").alias("max_revenue")
    ) \
    .orderBy("clv_band") \
    .show()

# Churn risk summary
churn_count = clv_df.filter(F.col("is_churn_risk") == True).count()
print(f"Churn risk customers (>45 days inactive): {churn_count:,}")

# ══════════════════════════════════════════════════════════════════════════
# SELECT FINAL COLUMNS AND WRITE
# ══════════════════════════════════════════════════════════════════════════
gold_clv = clv_df.select(
    # Customer identifier
    "user_id",

    # Revenue metrics
    "total_gross_revenue",
    "total_net_revenue",
    "total_discounts_received",

    # Order behaviour
    "total_orders",
    "total_line_items",
    "avg_order_item_value",

    # Time metrics
    "first_order_date",
    "last_order_date",
    "days_as_customer",
    "days_since_last_order",
    "active_days",
    "avg_order_frequency_days",

    # Loyalty
    "is_loyalty_member",
    "loyalty_orders",

    # Location
    "unique_restaurants_visited",

    # CLV outputs
    "revenue_percentile",
    "clv_band",
    "is_churn_risk",

    # Metadata
    "snapshot_date",
    "pipeline_run_ts",
    "pipeline_run_date",
)

# Write partitioned by snapshot_date — enables time-series CLV analysis
gold_clv.write \
    .mode("overwrite") \
    .partitionBy("snapshot_date") \
    .parquet(f"{GOLD_PATH}/clv/")

print(f"\n✅ Gold CLV written: {total_customers:,} customers")
print(f"   Path: {GOLD_PATH}/clv/snapshot_date={SNAPSHOT_DT}/")

job.commit()
print(f"\n🎉 Gold CLV job complete: {datetime.now(timezone.utc).isoformat()}")