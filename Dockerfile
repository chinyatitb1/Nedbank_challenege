FROM nedbank-de-challenge/base:1.0

# Install any additional Python dependencies you need beyond the base image.
# Leave requirements.txt empty if the base packages are sufficient.
WORKDIR /app
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PYSPARK_PYTHON=python3
ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_LOCAL_HOSTNAME=localhost
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download Delta Lake JARs so the container works with --network=none.
# delta-spark==3.1.0 requires these two JARs from Maven Central.
RUN mkdir -p /app/jars \
    && curl -fsSL "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar" \
         -o /app/jars/delta-spark.jar \
    && curl -fsSL "https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar" \
         -o /app/jars/delta-storage.jar

# Copy pipeline code and configuration into the image.
# Do NOT copy data files or output directories — these are injected at runtime
# via Docker volume mounts by the scoring system.
COPY pipeline/ pipeline/
COPY config/ config/
COPY validate_gold.py validate_gold.py

# Entry point — must run the complete pipeline end-to-end without interactive input.
# The scoring system uses this CMD directly; do not require TTY or stdin.
CMD ["python", "pipeline/run_all.py"]
