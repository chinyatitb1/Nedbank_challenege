"""
Data quality detection and flagging functions for Stage 2.

Each detect_* function returns (result_df, count) where:
  - result_df  is the DataFrame to continue processing (flagged or filtered)
  - count      is the number of affected records (for the DQ report)

Excluded records are logged, not written to quarantine tables.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.logger import get_logger

LOGGER = get_logger(__name__)


def detect_null_pk(df: DataFrame, pk_column: str) -> tuple[DataFrame, int]:
    """Exclude records where the primary key is null (NULL_REQUIRED)."""
    null_count = df.filter(F.col(pk_column).isNull()).count()
    if null_count > 0:
        LOGGER.info("NULL_REQUIRED: %d records with null %s excluded", null_count, pk_column)
    clean_df = df.filter(F.col(pk_column).isNotNull())
    return clean_df, null_count


def detect_date_format(df: DataFrame, column_name: str) -> tuple[DataFrame, int]:
    """Count records with non-ISO date formats. The dates are normalised by
    parse_date_column (already in utils.py), so we only need to detect them
    before normalisation to get the count.

    A non-ISO record is one where the raw string does NOT match yyyy-MM-dd
    but DOES successfully parse via one of the fallback formats.
    """
    iso_pattern = r"^\d{4}-\d{2}-\d{2}$"
    non_iso_count = df.filter(
        F.col(column_name).isNotNull()
        & ~F.col(column_name).rlike(iso_pattern)
    ).count()
    if non_iso_count > 0:
        LOGGER.info("DATE_FORMAT: %d non-ISO values in column %s", non_iso_count, column_name)
    return df, non_iso_count


def detect_currency_variants(df: DataFrame, variant_map: dict[str, str]) -> tuple[DataFrame, int]:
    """Detect and normalise non-ZAR currency representations.
    Returns flagged df (currency set to ZAR, _dq_currency_variant marker added)
    and the count of affected records.
    """
    variant_keys = list(variant_map.keys())
    is_variant = F.col("currency").isin(variant_keys) | (
        F.upper(F.col("currency")).isin([v.upper() for v in variant_keys])
        & ~F.col("currency").eqNullSafe("ZAR")
    )
    # Simpler: anything that is not null, not empty, and not already "ZAR"
    is_variant = (
        F.col("currency").isNotNull()
        & (F.col("currency") != "ZAR")
    )
    variant_count = df.filter(is_variant).count()
    if variant_count > 0:
        LOGGER.info("CURRENCY_VARIANT: %d non-ZAR currency values normalised", variant_count)
    # Mark before normalising
    df = df.withColumn("_dq_currency_variant", is_variant)
    # Normalise all to ZAR
    df = df.withColumn("currency", F.lit("ZAR"))
    return df, variant_count


def detect_type_mismatch(df: DataFrame) -> tuple[DataFrame, int]:
    """Detect transactions where the JSON amount was delivered as a string.
    Relies on _amount_is_string column added during Bronze ingestion.
    Falls back to 0 if the column is absent (Stage 1 data).
    """
    if "_amount_is_string" not in df.columns:
        return df, 0
    mismatch_count = df.filter(F.col("_amount_is_string")).count()
    if mismatch_count > 0:
        LOGGER.info("TYPE_MISMATCH: %d transactions with string-typed amount", mismatch_count)
    # Mark for flagging; the actual cast happens in transform.py
    df = df.withColumn("_dq_type_mismatch", F.col("_amount_is_string"))
    return df, mismatch_count


def detect_duplicates(df: DataFrame) -> tuple[DataFrame, int]:
    """Detect duplicate transaction_ids. Returns:
      - deduped_df: one row per transaction_id (earliest timestamp kept)
      - count of duplicate records (extras beyond the first)
    """
    from pyspark.sql import Window

    window = Window.partitionBy("transaction_id").orderBy(
        F.col("transaction_timestamp").asc_nulls_last(),
        F.col("ingestion_timestamp").asc_nulls_last(),
    )
    ranked = df.withColumn("_dup_rank", F.row_number().over(window))
    dup_count = ranked.filter(F.col("_dup_rank") > 1).count()
    if dup_count > 0:
        LOGGER.info("DUPLICATE_DEDUPED: %d duplicate transaction records excluded (keep first)", dup_count)
    # Mark the first-ranked row of a duplicate group
    dup_group_flag = Window.partitionBy("transaction_id")
    ranked = ranked.withColumn("_dup_group_size", F.count("*").over(dup_group_flag))
    ranked = ranked.withColumn(
        "_dq_duplicate_deduped",
        F.col("_dup_group_size") > 1,
    )
    deduped_df = ranked.filter(F.col("_dup_rank") == 1).drop("_dup_rank", "_dup_group_size")
    return deduped_df, dup_count


def detect_orphaned_transactions(
    transactions_df: DataFrame, accounts_df: DataFrame
) -> tuple[DataFrame, int]:
    """Detect transactions whose account_id is not in accounts.
    Returns clean transactions (with valid account_id) and the count of orphans.
    """
    valid_account_ids = F.broadcast(accounts_df.select("account_id").distinct())
    orphaned = transactions_df.join(valid_account_ids, on="account_id", how="left_anti")
    orphan_count = orphaned.count()
    if orphan_count > 0:
        LOGGER.info("ORPHANED_ACCOUNT: %d transactions with unknown account_id excluded", orphan_count)
    clean = transactions_df.join(valid_account_ids, on="account_id", how="inner")
    return clean, orphan_count


def apply_dq_flag_precedence(df: DataFrame, precedence: list[str]) -> DataFrame:
    """Set the dq_flag column based on individual DQ marker columns.
    Precedence order determines which flag wins when multiple apply.
    """
    marker_map = {
        "DUPLICATE_DEDUPED": "_dq_duplicate_deduped",
        "TYPE_MISMATCH": "_dq_type_mismatch",
        "DATE_FORMAT": "_dq_date_format",
        "CURRENCY_VARIANT": "_dq_currency_variant",
    }
    # Build a coalesce-like CASE expression in precedence order
    expr = F.lit(None).cast(T.StringType())
    for flag in reversed(precedence):
        col_name = marker_map.get(flag)
        if col_name and col_name in df.columns:
            expr = F.when(F.col(col_name), F.lit(flag)).otherwise(expr)

    df = df.withColumn("dq_flag", expr)
    # Drop internal marker columns
    drop_cols = [c for c in marker_map.values() if c in df.columns]
    return df.drop(*drop_cols)
