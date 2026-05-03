from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DecimalType, IntegerType, BooleanType

def apply_dq_flags(df):
    """Apply data quality flags based on business rules."""
    return df.withColumn("dq_flag",
        F.when(F.col("lineitem_id").isNull(),
            F.lit("NULL_LINEITEM_ID"))
         .when(F.col("order_id").isNull(),
            F.lit("NULL_ORDER_ID"))
         .when(F.col("user_id").isNull(),
            F.lit("NULL_USER_ID"))
         .when(F.col("printed_card_number").isNull(),
            F.lit("NULL_CARD_NUMBER"))
         .when(F.col("item_price").isNull(),
            F.lit("NULL_PRICE"))
         .when(F.col("item_price") < 0,
            F.lit("NEGATIVE_PRICE"))
         .when(F.col("item_quantity") <= 0,
            F.lit("INVALID_QUANTITY"))
         .when(F.col("creation_time_utc").isNull(),
            F.lit("NULL_TIMESTAMP"))
         .otherwise(F.lit("CLEAN"))
    )

def hard_drop_critical_nulls(df):
    """
    Drop only rows where critical keys are null.
    Returns (kept_df, dropped_df) tuple.
    """
    critical_flags = ["NULL_LINEITEM_ID", "NULL_ORDER_ID"]
    dropped = df.filter(F.col("dq_flag").isin(critical_flags))
    kept    = df.filter(~F.col("dq_flag").isin(critical_flags))
    return kept, dropped

def deduplicate_order_items(df):
    """
    Deduplicate using lineitem_id as PK.
    Keeps latest record by ingestion_ts.
    """
    window = Window \
        .partitionBy("lineitem_id") \
        .orderBy(F.col("ingestion_ts").desc())
    return df \
        .withColumn("row_num", F.row_number().over(window)) \
        .filter(F.col("row_num") == 1) \
        .drop("row_num")

def deduplicate_order_item_options(df):
    """
    Deduplicate using composite PK:
    (lineitem_id, option_group_name, option_name)
    """
    window = Window \
        .partitionBy("lineitem_id", "option_group_name", "option_name") \
        .orderBy(F.col("ingestion_ts").desc())
    return df \
        .withColumn("row_num", F.row_number().over(window)) \
        .filter(F.col("row_num") == 1) \
        .drop("row_num")

def add_customer_eligibility(df):
    """
    Mark rows eligible for customer-level metrics (CLV, RFM, churn).
    Only requires user_id — printed_card_number nulls do NOT disqualify.
    """
    return df.withColumn("eligible_for_customer_metrics",
        F.when(F.col("user_id").isNotNull(), F.lit(True))
         .otherwise(F.lit(False))
    )

def compute_revenue(df):
    """Compute revenue as item_price * item_quantity."""
    return df.withColumn("revenue",
        (F.col("item_price") * F.col("item_quantity"))
        .cast(DecimalType(12, 2))
    )

def cast_order_items(df):
    """Cast all columns to correct types."""
    return df \
        .withColumn("item_price",
            F.col("item_price").cast(DecimalType(10, 2))) \
        .withColumn("item_quantity",
            F.col("item_quantity").cast(IntegerType())) \
        .withColumn("is_loyalty",
            F.col("is_loyalty").cast(BooleanType())) \
        .withColumn("order_date",
            F.to_date(F.col("creation_time_utc")))