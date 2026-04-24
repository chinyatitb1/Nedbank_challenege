"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Input paths (Silver layer output — read these, do not modify):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Output paths (your pipeline must create these directories):
  /data/output/gold/fact_transactions/     — 15 fields (see output_schema_spec.md §2)
  /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
  /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)

Requirements:
  - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
    across pipeline re-runs on the same input data. Use row_number() with a
    stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
  - Resolve all foreign key relationships:
      fact_transactions.account_sk  → dim_accounts.account_sk
      fact_transactions.customer_sk → dim_customers.customer_sk
      dim_accounts.customer_id      → dim_customers.customer_id
  - Rename accounts.customer_ref → dim_accounts.customer_id at this layer.
  - Derive dim_customers.age_band from dob (do not copy dob directly).
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.
  - At Stage 2, also write /data/output/dq_report.json summarising DQ outcomes.

See output_schema_spec.md for the complete field-by-field specification.
"""

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import broadcast

from pipeline.config import load_config
from pipeline.dq import detect_orphaned_transactions
from pipeline.logger import get_logger
from pipeline.mappings import (
    DIM_ACCOUNTS_COLUMNS,
    DIM_ACCOUNTS_RENAMES,
    DIM_CUSTOMERS_COLUMNS,
)
from pipeline.spark_utils import get_spark_session
from pipeline.utils import age_band_from_dob, delta_read, delta_write, stable_surrogate_key, table_path


LOGGER = get_logger(__name__)


def apply_renames(df, rename_map: dict[str, str]):
    for source_name, target_name in rename_map.items():
        df = df.withColumnRenamed(source_name, target_name)
    return df


def run_provisioning() -> tuple[dict[str, int], dict[str, int], dict]:
    """Run Gold provisioning. Returns (gold_counts, dq_counts)."""
    config = load_config()
    spark = get_spark_session(config)

    silver_root = config["output"]["silver_path"]
    gold_root = config["output"]["gold_path"]

    dq_counts: dict[str, int] = {}

    LOGGER.info("Starting Gold provisioning")

    customers_df = delta_read(spark, table_path(silver_root, "customers"))
    accounts_df = delta_read(spark, table_path(silver_root, "accounts"))
    transactions_df = delta_read(spark, table_path(silver_root, "transactions"))

    # ── ORPHANED_ACCOUNT: explicitly detect and exclude ───────
    # This replaces the silent inner-join drop with a counted, logged exclusion.
    transactions_df, orphan_count = detect_orphaned_transactions(
        transactions_df, accounts_df
    )
    dq_counts["ORPHANED_ACCOUNT"] = orphan_count

    # ── dim_customers ─────────────────────────────────────────
    dim_customers = (
        customers_df.withColumn("customer_sk", stable_surrogate_key(F.col("customer_id")).cast(T.LongType()))
        .withColumn("age_band", age_band_from_dob("dob"))
        .select(*DIM_CUSTOMERS_COLUMNS)
    )
    delta_write(dim_customers, table_path(gold_root, "dim_customers"))

    # ── dim_accounts ──────────────────────────────────────────
    # Inner join ensures every dim_accounts row has a valid customer reference.
    dim_accounts = (
        apply_renames(
            accounts_df.withColumn("account_sk", stable_surrogate_key(F.col("account_id")).cast(T.LongType())),
            DIM_ACCOUNTS_RENAMES,
        )
        .join(broadcast(dim_customers.select("customer_id")), on="customer_id", how="inner")
        .select(*DIM_ACCOUNTS_COLUMNS)
    )
    delta_write(dim_accounts, table_path(gold_root, "dim_accounts"))

    # ── fact_transactions ─────────────────────────────────────
    account_lookup = dim_accounts.select(
        F.col("account_id"),
        F.col("account_sk"),
        F.col("customer_id"),
    ).join(
        broadcast(dim_customers.select("customer_id", "customer_sk")),
        on="customer_id",
        how="inner",
    )

    fact_transactions = (
        transactions_df.alias("t")
        .join(broadcast(account_lookup).alias("a"), on="account_id", how="inner")
        .withColumn("transaction_sk", stable_surrogate_key(F.col("transaction_id")).cast(T.LongType()))
        .select(
            "transaction_sk",
            "transaction_id",
            F.col("a.account_sk").alias("account_sk"),
            F.col("a.customer_sk").alias("customer_sk"),
            "transaction_date",
            "transaction_timestamp",
            "transaction_type",
            "merchant_category",
            "merchant_subcategory",
            "amount",
            "currency",
            "channel",
            F.col("t.location.province").alias("province"),
            "dq_flag",
            "ingestion_timestamp",
        )
    )
    delta_write(fact_transactions, table_path(gold_root, "fact_transactions"))

    # Compute gold counts and flag statistics from fact_transactions
    total_ft = fact_transactions.count()
    flag_counts_rows = (
        fact_transactions.filter(F.col("dq_flag").isNotNull())
        .groupBy("dq_flag")
        .count()
        .collect()
    )
    flag_counts = {row["dq_flag"]: row["count"] for row in flag_counts_rows}
    flagged_total = sum(flag_counts.values())

    gold_counts = {
        "fact_transactions": total_ft,
        "dim_accounts": dim_accounts.count(),
        "dim_customers": dim_customers.count(),
    }
    flag_stats = {
        "total_records": total_ft,
        "clean_records": total_ft - flagged_total,
        "flagged_records": flagged_total,
        "flag_counts": flag_counts,
    }

    LOGGER.info("Gold provisioning completed — gold counts: %s, flag stats: %s", gold_counts, flag_stats)
    return gold_counts, dq_counts, flag_stats
