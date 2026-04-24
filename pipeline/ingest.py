"""
Bronze layer: ingest raw source data into Delta tables.
"""

from pyspark.sql import functions as F

from pipeline.config import load_config
from pipeline.logger import get_logger
from pipeline.schemas import ACCOUNTS_SCHEMA, CUSTOMERS_SCHEMA, TRANSACTIONS_SCHEMA
from pipeline.spark_utils import get_spark_session
from pipeline.utils import add_ingestion_timestamp, current_run_timestamp_literal, delta_write, table_path


LOGGER = get_logger(__name__)


def run_ingestion() -> dict[str, int]:
    """Run Bronze ingestion. Returns raw record counts per source file."""
    config = load_config()
    spark = get_spark_session(config)

    input_config = config["input"]
    bronze_root = config["output"]["bronze_path"]
    run_timestamp = current_run_timestamp_literal()

    raw_counts: dict[str, int] = {}

    LOGGER.info("Starting Bronze ingestion")

    # ── Accounts ──────────────────────────────────────────────
    accounts_df = spark.read.option("header", "true").schema(ACCOUNTS_SCHEMA).csv(input_config["accounts_path"])
    raw_counts["accounts_raw"] = accounts_df.count()
    delta_write(
        add_ingestion_timestamp(accounts_df, run_timestamp),
        table_path(bronze_root, "accounts"),
    )

    # ── Customers ─────────────────────────────────────────────
    customers_df = spark.read.option("header", "true").schema(CUSTOMERS_SCHEMA).csv(input_config["customers_path"])
    raw_counts["customers_raw"] = customers_df.count()
    delta_write(
        add_ingestion_timestamp(customers_df, run_timestamp),
        table_path(bronze_root, "customers"),
    )

    # ── Transactions ──────────────────────────────────────────
    # Read as raw text first to detect TYPE_MISMATCH (amount delivered as JSON string).
    # A JSON string amount looks like "amount": "349.50" (quoted value).
    # A JSON numeric amount looks like "amount": 349.50 (no quotes).
    raw_lines = spark.read.text(input_config["transactions_path"])
    raw_counts["transactions_raw"] = raw_lines.count()

    raw_lines = raw_lines.withColumn(
        "_amount_is_string",
        # Match "amount" followed by colon and a quoted string value
        F.col("value").rlike(r'"amount"\s*:\s*"'),
    )

    # Parse the JSON line into the existing schema
    parsed = raw_lines.withColumn("_parsed", F.from_json(F.col("value"), TRANSACTIONS_SCHEMA))

    # Flatten: select all schema fields + keep _amount_is_string
    transactions_df = parsed.select(
        F.col("_parsed.*"),
        F.col("_amount_is_string"),
    )

    delta_write(
        add_ingestion_timestamp(transactions_df, run_timestamp),
        table_path(bronze_root, "transactions"),
    )

    LOGGER.info("Bronze ingestion completed — raw counts: %s", raw_counts)
    return raw_counts
