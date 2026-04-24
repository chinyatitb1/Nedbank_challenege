"""
Bronze layer: ingest raw source data into Delta tables.
"""

from pipeline.config import load_config
from pipeline.logger import get_logger
from pipeline.schemas import ACCOUNTS_SCHEMA, CUSTOMERS_SCHEMA, TRANSACTIONS_SCHEMA
from pipeline.spark_utils import get_spark_session
from pipeline.utils import add_ingestion_timestamp, current_run_timestamp_literal, delta_write, table_path


LOGGER = get_logger(__name__)


def run_ingestion():
    config = load_config()
    spark = get_spark_session(config)

    input_config = config["input"]
    bronze_root = config["output"]["bronze_path"]
    run_timestamp = current_run_timestamp_literal()

    LOGGER.info("Starting Bronze ingestion")

    accounts_df = spark.read.option("header", "true").schema(ACCOUNTS_SCHEMA).csv(input_config["accounts_path"])
    delta_write(
        add_ingestion_timestamp(accounts_df, run_timestamp),
        table_path(bronze_root, "accounts"),
    )

    customers_df = spark.read.option("header", "true").schema(CUSTOMERS_SCHEMA).csv(input_config["customers_path"])
    delta_write(
        add_ingestion_timestamp(customers_df, run_timestamp),
        table_path(bronze_root, "customers"),
    )

    transactions_df = spark.read.schema(TRANSACTIONS_SCHEMA).json(input_config["transactions_path"])
    delta_write(
        add_ingestion_timestamp(transactions_df, run_timestamp),
        table_path(bronze_root, "transactions"),
    )

    LOGGER.info("Bronze ingestion completed")
