"""
Silver layer: standardize and deduplicate Bronze tables.
"""

from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.config import load_config
from pipeline.logger import get_logger
from pipeline.spark_utils import get_spark_session
from pipeline.utils import deduplicate, delta_read, delta_write, parse_date_column, table_path


LOGGER = get_logger(__name__)


def run_transformation():
    config = load_config()
    spark = get_spark_session(config)

    bronze_root = config["output"]["bronze_path"]
    silver_root = config["output"]["silver_path"]

    LOGGER.info("Starting Silver transformation")

    accounts_df = delta_read(spark, table_path(bronze_root, "accounts"))
    accounts_df = (
        accounts_df.withColumn("open_date", parse_date_column("open_date"))
        .withColumn("last_activity_date", parse_date_column("last_activity_date"))
        .withColumn("credit_limit", F.col("credit_limit").cast(T.DecimalType(18, 2)))
        .withColumn("current_balance", F.col("current_balance").cast(T.DecimalType(18, 2)))
    )
    accounts_df = deduplicate(accounts_df, ["account_id"], ["ingestion_timestamp"])
    delta_write(accounts_df, table_path(silver_root, "accounts"))

    customers_df = delta_read(spark, table_path(bronze_root, "customers"))
    customers_df = customers_df.withColumn("dob", parse_date_column("dob"))
    customers_df = deduplicate(customers_df, ["customer_id"], ["ingestion_timestamp"])
    delta_write(customers_df, table_path(silver_root, "customers"))

    transactions_df = delta_read(spark, table_path(bronze_root, "transactions"))
    transactions_df = (
        transactions_df.withColumn("transaction_date", parse_date_column("transaction_date"))
        .withColumn("amount", F.col("amount").cast(T.DecimalType(18, 2)))
        .withColumn("currency", F.lit("ZAR"))
        .withColumn("merchant_subcategory", F.col("merchant_subcategory").cast(T.StringType()))
        .withColumn("dq_flag", F.lit(None).cast(T.StringType()))
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.date_format(F.col("transaction_date"), "yyyy-MM-dd"),
                    F.col("transaction_time"),
                ),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
    )
    transactions_df = deduplicate(
        transactions_df,
        ["transaction_id"],
        ["transaction_timestamp", "ingestion_timestamp"],
    )
    delta_write(
        transactions_df.select(
            "transaction_id",
            "account_id",
            "transaction_date",
            "transaction_time",
            "transaction_timestamp",
            "transaction_type",
            "merchant_category",
            "merchant_subcategory",
            "amount",
            "currency",
            "channel",
            "location",
            "metadata",
            "dq_flag",
            "ingestion_timestamp",
        ),
        table_path(silver_root, "transactions"),
    )

    LOGGER.info("Silver transformation completed")
