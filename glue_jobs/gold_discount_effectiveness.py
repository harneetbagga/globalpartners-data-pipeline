"""
Gold Discount Effectiveness Job
This job analyses how discounts impact order behaviour — whether discounted orders have higher basket sizes, whether discount customers return more, and which item categories drive the most discount usage.
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

print(f"Gold Discount Effectiveness job started: {RUN_TS.isoformat()}")

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

print(f"order_items rows:         {silver_oi.count():,}")
print(f"order_item_options rows:  {silver_oio.count():,}")

# ══════════════════════════════════════════════════════════════════════════
# STEP 1 — IDENTIFY DISCOUNTED VS NON-DISCOUNTED LINE ITEMS
# ══════════════════════════════════════════════════════════════════════════
# Aggregate options per lineitem — detect if any discount was applied
option_summary = silver_oio.groupBy("order_id", "lineitem_id") \
    .agg(
        F.sum("option_price").cast(DecimalType(12, 2))
         .alias("total_option_price"),
        F.sum(F.when(F.col("is_discount") == True, F.col("option_price"))
              .otherwise(0)).cast(DecimalType(12, 2))
         .alias("total_discount_amount"),
        F.sum(F.when(F.col("is_discount") == True, 1)
              .otherwise(0)).alias("discount_count"),
        F.max(F.col("is_discount").cast("int")).cast("boolean")
         .alias("has_discount"),
    )

# Join options onto order items
enriched = silver_oi.join(
    option_summary,
    on  = ["order_id", "lineitem_id"],
    how = "left"
).fillna({
    "total_option_price": 0,
    "total_discount_amount": 0,
    "discount_count": 0,
    "has_discount": False
}) \
.withColumn("gross_revenue",
    F.col("revenue").cast(DecimalType(12, 2))
) \
.withColumn("net_revenue",
    (F.col("revenue") + F.col("total_option_price"))
    .cast(DecimalType(12, 2))
) \
.withColumn("discount_depth_pct",
    F.when(
        F.col("revenue") > 0,
        (F.abs(F.col("total_discount_amount")) / F.col("revenue") * 100)
        .cast(DecimalType(6, 2))
    ).otherwise(F.lit(0))
)

# ══════════════════════════════════════════════════════════════════════════
# STEP 2 — ORDER-LEVEL DISCOUNT FLAGS
# ══════════════════════════════════════════════════════════════════════════
# Flag each order as discounted if ANY line item had a discount
order_discount_flags = enriched.groupBy("order_id") \
    .agg(
        F.max(F.col("has_discount").cast("int")).cast("boolean")
         .alias("order_has_discount"),
        F.sum("total_discount_amount").cast(DecimalType(12, 2))
         .alias("order_total_discount"),
    )

enriched = enriched.join(order_discount_flags, on="order_id", how="left")

# ══════════════════════════════════════════════════════════════════════════
# STEP 3 — DISCOUNT vs NON-DISCOUNT ORDER COMPARISON
# ══════════════════════════════════════════════════════════════════════════
order_comparison = enriched.groupBy("order_id", "order_has_discount",
                                     "restaurant_id", "order_date") \
    .agg(
        F.sum("gross_revenue").cast(DecimalType(12, 2)).alias("order_gross_revenue"),
        F.sum("net_revenue").cast(DecimalType(12, 2)).alias("order_net_revenue"),
        F.sum("total_discount_amount").cast(DecimalType(12, 2))
         .alias("order_discount_amount"),
        F.count("lineitem_id").alias("items_in_order"),
        F.first("user_id").alias("user_id"),
        F.first("is_loyalty").alias("is_loyalty"),
    )

discount_order_summary = order_comparison.groupBy("order_has_discount") \
    .agg(
        F.count("order_id").alias("total_orders"),
        F.round(F.avg("order_gross_revenue"), 2).alias("avg_gross_order_value"),
        F.round(F.avg("order_net_revenue"), 2).alias("avg_net_order_value"),
        F.round(F.avg("items_in_order"), 2).alias("avg_items_per_order"),
        F.round(F.avg("order_discount_amount"), 2).alias("avg_discount_amount"),
        F.round(F.sum("order_gross_revenue"), 2).alias("total_gross_revenue"),
        F.round(F.sum("order_net_revenue"), 2).alias("total_net_revenue"),
        F.countDistinct("user_id").alias("unique_customers"),
        F.round(
            F.sum(F.col("order_has_discount").cast("int")) /
            F.count("order_id") * 100, 2
        ).alias("discount_order_rate_pct"),
    ) \
    .withColumn("discount_label",
        F.when(F.col("order_has_discount") == True, F.lit("Discounted"))
         .otherwise(F.lit("Non-Discounted"))
    ) \
    .withColumn("snapshot_date",     F.lit(SNAPSHOT_DT)) \
    .withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
    .withColumn("pipeline_run_date", F.lit(SNAPSHOT_DT))

# ══════════════════════════════════════════════════════════════════════════
# STEP 4 — DISCOUNT EFFECTIVENESS BY CATEGORY
# ══════════════════════════════════════════════════════════════════════════
category_discount = enriched \
    .filter(F.col("item_category").isNotNull()) \
    .groupBy("item_category", "has_discount") \
    .agg(
        F.count("lineitem_id").alias("total_line_items"),
        F.sum("gross_revenue").cast(DecimalType(12, 2)).alias("total_gross_revenue"),
        F.sum("net_revenue").cast(DecimalType(12, 2)).alias("total_net_revenue"),
        F.round(F.avg("gross_revenue"), 2).alias("avg_item_price"),
        F.round(F.avg("discount_depth_pct"), 2).alias("avg_discount_depth_pct"),
        F.sum(F.abs(F.col("total_discount_amount")))
         .cast(DecimalType(12, 2)).alias("total_discount_given"),
        F.countDistinct("order_id").alias("total_orders"),
    ) \
    .withColumn("discount_label",
        F.when(F.col("has_discount") == True, F.lit("Discounted"))
         .otherwise(F.lit("Non-Discounted"))
    ) \
    .withColumn("snapshot_date",     F.lit(SNAPSHOT_DT)) \
    .withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
    .withColumn("pipeline_run_date", F.lit(SNAPSHOT_DT))

# ══════════════════════════════════════════════════════════════════════════
# STEP 5 — CUSTOMER DISCOUNT BEHAVIOUR
# ══════════════════════════════════════════════════════════════════════════
# For customer-eligible rows only
customer_discount = enriched \
    .filter(F.col("eligible_for_customer_metrics") == True) \
    .groupBy("user_id") \
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct(
            F.when(F.col("order_has_discount") == True, F.col("order_id"))
        ).alias("discounted_orders"),
        F.sum("gross_revenue").cast(DecimalType(12, 2)).alias("total_gross_spend"),
        F.sum("net_revenue").cast(DecimalType(12, 2)).alias("total_net_spend"),
        F.sum(F.abs(F.col("total_discount_amount")))
         .cast(DecimalType(12, 2)).alias("total_discounts_received"),
        F.max(F.col("is_loyalty").cast("int")).cast("boolean")
         .alias("is_loyalty_member"),
    ) \
    .withColumn("discount_usage_rate",
        (F.col("discounted_orders") / F.col("total_orders") * 100)
        .cast(DecimalType(5, 2))
    ) \
    .withColumn("discount_seeker_segment",
        F.when(F.col("discount_usage_rate") >= 75, F.lit("HEAVY_DISCOUNT_USER"))
         .when(F.col("discount_usage_rate") >= 25, F.lit("MODERATE_DISCOUNT_USER"))
         .when(F.col("discount_usage_rate") > 0,   F.lit("OCCASIONAL_DISCOUNT_USER"))
         .otherwise(F.lit("NON_DISCOUNT_USER"))
    ) \
    .withColumn("snapshot_date",     F.lit(SNAPSHOT_DT)) \
    .withColumn("pipeline_run_ts",   F.lit(RUN_TS.isoformat())) \
    .withColumn("pipeline_run_date", F.lit(SNAPSHOT_DT))

# ══════════════════════════════════════════════════════════════════════════
# LOG SUMMARY
# ══════════════════════════════════════════════════════════════════════════
print("\nDiscount vs Non-Discount Order Comparison:")
discount_order_summary.select(
    "discount_label", "total_orders", "avg_gross_order_value",
    "avg_net_order_value", "avg_items_per_order", "avg_discount_amount"
).show(truncate=False)

print("\nDiscount Seeker Segments:")
customer_discount.groupBy("discount_seeker_segment") \
    .agg(
        F.count("user_id").alias("customers"),
        F.round(F.avg("total_net_spend"), 2).alias("avg_spend"),
        F.round(F.avg("discount_usage_rate"), 1).alias("avg_discount_rate")
    ) \
    .orderBy(F.col("customers").desc()) \
    .show(truncate=False)

print("\nTop 5 Categories by Discount Given:")
category_discount \
    .filter(F.col("has_discount") == True) \
    .orderBy(F.col("total_discount_given").desc()) \
    .select("item_category", "total_discount_given",
            "avg_discount_depth_pct", "total_orders") \
    .show(5, truncate=False)

# ══════════════════════════════════════════════════════════════════════════
# WRITE GOLD DISCOUNT EFFECTIVENESS
# ══════════════════════════════════════════════════════════════════════════
discount_order_summary.write \
    .mode("overwrite") \
    .partitionBy("snapshot_date") \
    .parquet(f"{GOLD_PATH}/discount_order_summary/")

category_discount.write \
    .mode("overwrite") \
    .partitionBy("snapshot_date") \
    .parquet(f"{GOLD_PATH}/discount_by_category/")

customer_discount.write \
    .mode("overwrite") \
    .partitionBy("snapshot_date") \
    .parquet(f"{GOLD_PATH}/discount_customer_behaviour/")

print(f"\n✅ Discount effectiveness written to Gold")
print(f"   Order summary:       {GOLD_PATH}/discount_order_summary/")
print(f"   By category:         {GOLD_PATH}/discount_by_category/")
print(f"   Customer behaviour:  {GOLD_PATH}/discount_customer_behaviour/")

job.commit()
print(f"\n🎉 Gold Discount Effectiveness complete: {datetime.now(timezone.utc).isoformat()}")