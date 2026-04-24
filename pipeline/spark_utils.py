import os
import shutil

from pyspark.sql import SparkSession


_spark_session: SparkSession | None = None

# Delta JARs are pre-downloaded into /app/jars/ during the Docker build so the
# container can run with --network=none (no Maven/Ivy downloads at runtime).
_DELTA_JARS = "/app/jars/delta-spark.jar,/app/jars/delta-storage.jar"


def get_spark_session(config: dict) -> SparkSession:
    global _spark_session

    if _spark_session is not None:
        return _spark_session

    # Ensure output subdirs exist — the scoring system mounts a fresh empty dir.
    for subdir in ("bronze", "silver", "gold"):
        os.makedirs(f"/data/output/{subdir}", exist_ok=True)

    # Spark shuffle/temp dir — use the output volume instead of /tmp (512 MB
    # tmpfs that overflows at Stage 2 scale).  Cleaned up in stop_spark_session.
    spark_tmp = "/data/output/.spark_tmp"
    os.makedirs(spark_tmp, exist_ok=True)

    spark_config = config.get("spark", {})
    builder = (
        SparkSession.builder.master(spark_config.get("master", "local[2]"))
        .appName(spark_config.get("app_name", "nedbank-de-pipeline"))
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "512m")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars", _DELTA_JARS)
        # Use uncompressed instead of Snappy so no native .so needs to be
        # extracted to /tmp, and DuckDB doesn't misinterpret the file
        # extension (e.g. .gz.parquet) as whole-file compression.
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        # Shuffle/spill to the output volume — /tmp is only 512 MB tmpfs.
        .config("spark.local.dir", spark_tmp)
    )

    _spark_session = builder.getOrCreate()
    _spark_session.sparkContext.setLogLevel("WARN")
    return _spark_session


def stop_spark_session() -> None:
    global _spark_session
    if _spark_session is not None:
        _spark_session.stop()
        _spark_session = None
    # Remove shuffle temp so it doesn't pollute the scored output tree.
    spark_tmp = "/data/output/.spark_tmp"
    if os.path.isdir(spark_tmp):
        shutil.rmtree(spark_tmp, ignore_errors=True)
