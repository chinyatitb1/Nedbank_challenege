"""
Silver layer: standardize, DQ-flag, and deduplicate Bronze tables.
"""

from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.config import load_config, load_dq_rules
from pipeline.dq import (
    apply_dq_flag_precedence,
    detect_currency_variants,
    detect_date_format,
    detect_duplicates,
    detect_null_pk,
    detect_type_mismatch,
)
from pipeline.logger import get_logger
from pipeline.spark_utils import get_spark_session
from pipeline.utils import delta_read, delta_write, parse_date_column, table_path


LOGGER = get_logger(__name__)


def run_transformation() -> dict[str, int]:
    """Run Silver transformation. Returns DQ issue counts."""
    config = load_config()
    dq_rules = load_dq_rules()
    spark = get_spark_session(config)

    bronze_root = config["output"]["bronze_path"]
    silver_root = config["output"]["silver_path"]
    rules = dq_rules.get("rules", {})
    precedence = dq_rules.get("dq_flag_precedence", [])
    variant_map = rules.get("CURRENCY_VARIANT", {}).get("variant_map", {})

    dq_counts: dict[str, int] = {}

    LOGGER.info("Starting Silver transformation")

    # ── Accounts ──────────────────────────────────────────────
    accounts_df = delta_read(spark, table_path(bronze_root, "accounts"))

    # NULL_REQUIRED: exclude records with null account_id
    accounts_df, null_pk_count = detect_null_pk(accounts_df, "account_id")
    dq_counts["NULL_REQUIRED"] = null_pk_count

    # DATE_FORMAT: detect non-ISO dates before normalising
    _, date_count_open = detect_date_format(accounts_df, "open_date")
    _, date_count_last = detect_date_format(accounts_df, "last_activity_date")

    accounts_df = (
        accounts_df.withColumn("open_date", parse_date_column("open_date"))
        .withColumn("last_activity_date", parse_date_column("last_activity_date"))
        .withColumn("credit_limit", F.col("credit_limit").cast(T.DecimalType(18, 2)))
        .withColumn("current_balance", F.col("current_balance").cast(T.DecimalType(18, 2)))
    )
    # Deduplicate accounts on account_id (keep latest by ingestion_timestamp)
    from pipeline.utils import deduplicate
    accounts_df = deduplicate(accounts_df, ["account_id"], ["ingestion_timestamp"])
    delta_write(accounts_df, table_path(silver_root, "accounts"))

    # ── Customers ─────────────────────────────────────────────
    customers_df = delta_read(spark, table_path(bronze_root, "customers"))

    # DATE_FORMAT: detect non-ISO dob
    _, date_count_dob = detect_date_format(customers_df, "dob")

    customers_df = customers_df.withColumn("dob", parse_date_column("dob"))
    customers_df = deduplicate(customers_df, ["customer_id"], ["ingestion_timestamp"])
    delta_write(customers_df, table_path(silver_root, "customers"))

    # Aggregate DATE_FORMAT across all files
    dq_counts["DATE_FORMAT"] = date_count_open + date_count_last + date_count_dob

    # ── Transactions ──────────────────────────────────────────
    transactions_df = delta_read(spark, table_path(bronze_root, "transactions"))

    # DATE_FORMAT: detect non-ISO transaction_date
    _, date_count_txn = detect_date_format(transactions_df, "transaction_date")
    dq_counts["DATE_FORMAT"] += date_count_txn

    # TYPE_MISMATCH: detect string-typed amount (uses _amount_is_string from Bronze)
    transactions_df, type_mismatch_count = detect_type_mismatch(transactions_df)
    dq_counts["TYPE_MISMATCH"] = type_mismatch_count

    # CURRENCY_VARIANT: detect and normalise non-ZAR values
    transactions_df, currency_count = detect_currency_variants(transactions_df, variant_map)
    dq_counts["CURRENCY_VARIANT"] = currency_count

    # Capture non-ISO date transaction_ids BEFORE parsing overwrites the raw values
    iso_pattern = r"^\d{4}-\d{2}-\d{2}$"
    date_flag_df = (
        transactions_df.filter(
            F.col("transaction_date").isNotNull() & ~F.col("transaction_date").rlike(iso_pattern)
        )
        .select("transaction_id")
        .distinct()
        .withColumn("_dq_date_format", F.lit(True))
    )

    # Parse dates and cast amount
    transactions_df = (
        transactions_df.withColumn("transaction_date", parse_date_column("transaction_date"))
        .withColumn("amount", F.col("amount").cast(T.DecimalType(18, 2)))
        .withColumn("merchant_subcategory", F.col("merchant_subcategory").cast(T.StringType()))
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

    # Left join to mark affected transaction_ids with date format flag
    transactions_df = transactions_df.join(
        F.broadcast(date_flag_df),
        on="transaction_id",
        how="left",
    ).withColumn(
        "_dq_date_format",
        F.coalesce(F.col("_dq_date_format"), F.lit(False)),
    )

    # DUPLICATE_DEDUPED: detect and deduplicate (keep earliest — ascending order)
    transactions_df, dup_count = detect_duplicates(transactions_df)
    dq_counts["DUPLICATE_DEDUPED"] = dup_count

    # Apply dq_flag precedence
    transactions_df = apply_dq_flag_precedence(transactions_df, precedence)

    # Select final Silver columns (drop _amount_is_string and other internal cols)
    silver_cols = [
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
    ]
    transactions_df = transactions_df.select(*silver_cols)

    delta_write(transactions_df, table_path(silver_root, "transactions"))

    LOGGER.info("Silver transformation completed — DQ counts: %s", dq_counts)
    return dq_counts
