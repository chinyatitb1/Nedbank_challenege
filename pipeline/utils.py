from datetime import datetime, timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def current_run_timestamp_literal():
    return F.lit(datetime.now(timezone.utc).replace(tzinfo=None)).cast(T.TimestampType())


def add_ingestion_timestamp(df: DataFrame, run_timestamp_literal) -> DataFrame:
    return df.withColumn("ingestion_timestamp", run_timestamp_literal)


def delta_write(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(path)
    )


def delta_read(spark, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def table_path(root_path: str, table_name: str) -> str:
    return f"{root_path.rstrip('/')}/{table_name}"


def stable_surrogate_key(*cols) -> F.Column:
    return F.pmod(F.xxhash64(*cols), F.lit(9223372036854775807))


def parse_date_column(column_name: str) -> F.Column:
    return F.coalesce(
        F.to_date(F.col(column_name), "yyyy-MM-dd"),
        F.to_date(F.col(column_name), "dd/MM/yyyy"),
        F.to_date(F.from_unixtime(F.col(column_name).cast("bigint"))),
    )


def age_band_from_dob(column_name: str) -> F.Column:
    age_years = F.floor(F.datediff(F.current_date(), F.col(column_name)) / F.lit(365.25))
    return (
        F.when(age_years >= 65, F.lit("65+"))
        .when(age_years >= 56, F.lit("56-65"))
        .when(age_years >= 46, F.lit("46-55"))
        .when(age_years >= 36, F.lit("36-45"))
        .when(age_years >= 26, F.lit("26-35"))
        .when(age_years >= 18, F.lit("18-25"))
    )


def deduplicate(df: DataFrame, key_columns: list[str], order_columns: list[str]) -> DataFrame:
    window = Window.partitionBy(*key_columns).orderBy(*[F.col(col).desc_nulls_last() for col in order_columns])
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )
