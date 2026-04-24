# Pipeline Architecture — End-to-End Documentation

> **Personal reference only — not committed to the repository.**

---

## Overview

A PySpark medallion pipeline (Bronze → Silver → Gold) running inside Docker with Delta Lake. Processes 3 source datasets (~1.2M total records) into a dimensional model that passes 3 DuckDB-scored validation queries.

```
/data/input/                      /data/output/
  accounts.csv (100K)     ──►       bronze/{accounts,customers,transactions}/
  customers.csv (80K)     ──►       silver/{accounts,customers,transactions}/
  transactions.jsonl (1M) ──►       gold/{fact_transactions,dim_accounts,dim_customers}/
```

---

## Entry Point

**`pipeline/run_all.py`** — Orchestrates the 3 layers sequentially:

```
run_ingestion()      →  Bronze (raw ingest)
run_transformation() →  Silver (standardise, dedup)
run_provisioning()   →  Gold   (dimensional model)
stop_spark_session()
```

Invoked via `CMD ["python", "pipeline/run_all.py"]` in the Dockerfile.

---

## Module Breakdown

### `pipeline/config.py` — Configuration loader

- Reads `pipeline_config.yaml` from `/data/config/` (injected at runtime by scorer)
- Falls back to `/app/config/` (baked into image) or `config/` (local dev)
- Supports `PIPELINE_CONFIG` env var override
- All input/output paths come from config — nothing is hardcoded

### `pipeline/spark_utils.py` — SparkSession management

- **`get_spark_session(config)`**: Singleton SparkSession with:
  - `local[2]` master (matches 2 vCPU constraint)
  - `executor.memory=1g`, `driver.memory=512m` (fits in 2GB RAM)
  - `shuffle.partitions=4`, `parallelism=2` (optimal for 2 cores)
  - AQE enabled for dynamic optimization
  - Delta Lake extensions via pre-downloaded JARs (`/app/jars/`)
  - **Uncompressed parquet** codec — avoids Snappy native lib issue on noexec tmpfs AND avoids DuckDB misinterpreting `.gz.parquet` as whole-file gzip
  - `spark.local.dir=/tmp` (uses the tmpfs mount)
  - Binds to `127.0.0.1` (works with `--network=none`)
- Creates `bronze/`, `silver/`, `gold/` output dirs before Spark starts

### `pipeline/schemas.py` — Source file schemas

- `ACCOUNTS_SCHEMA`: 11 fields from accounts.csv (all StringType for raw ingest except where types are known)
- `CUSTOMERS_SCHEMA`: 12 fields from customers.csv (risk_score is IntegerType)
- `TRANSACTIONS_SCHEMA`: Nested schema with `location` and `metadata` sub-structs

### `pipeline/mappings.py` — Column definitions

- `DIM_ACCOUNTS_RENAMES`: `customer_ref` → `customer_id` (GAP-026 fix)
- `DIM_ACCOUNTS_COLUMNS`: 11 fields matching output_schema_spec §3
- `DIM_CUSTOMERS_COLUMNS`: 9 fields matching output_schema_spec §4
- `FACT_TRANSACTIONS_COLUMNS`: 15 fields matching output_schema_spec §2

### `pipeline/utils.py` — Shared utilities

| Function | Purpose |
|---|---|
| `current_run_timestamp_literal()` | UTC timestamp for consistent ingestion_timestamp across all records in a run |
| `add_ingestion_timestamp(df, ts)` | Adds the ingestion_timestamp column |
| `delta_write(df, path, mode)` | Writes DataFrame as Delta table with `overwriteSchema=true` |
| `delta_read(spark, path)` | Reads Delta table |
| `table_path(root, table)` | Constructs `root/table` path |
| `stable_surrogate_key(*cols)` | `xxhash64(*cols) % MAX_LONG` — deterministic, stable across re-runs |
| `parse_date_column(col)` | Coalesces `yyyy-MM-dd`, `dd/MM/yyyy`, and epoch formats |
| `age_band_from_dob(col)` | Derives age band buckets (18-25, 26-35, ... 65+) from DOB |
| `deduplicate(df, keys, order)` | `row_number()` window dedup — keeps latest by order columns |

### `pipeline/logger.py` — Logging

- Simple stdout StreamHandler with `%(asctime)s %(levelname)s %(name)s - %(message)s` format
- INFO level, no propagation to root logger

---

## Layer 1: Bronze (`pipeline/ingest.py`)

**`run_ingestion()`**

1. Reads config paths for input files
2. Generates a single `run_timestamp` for the entire batch
3. For each source file:
   - Reads with explicit schema (no schema inference)
   - Adds `ingestion_timestamp` column
   - Writes as Delta table to `bronze/{table}/`
4. **No transformation** — data preserved as-is from source

**Key decisions:**
- Schema enforcement at read prevents bad data from silently entering the pipeline
- Single timestamp ensures all Bronze records from the same run are identifiable
- Reading CSV with explicit header + schema avoids inference overhead

---

## Layer 2: Silver (`pipeline/transform.py`)

**`run_transformation()`**

For each table:

**accounts:**
- `open_date`, `last_activity_date` → parsed via `parse_date_column()` (multi-format coalesce)
- `credit_limit`, `current_balance` → cast to `DECIMAL(18,2)`
- Deduplicates on `account_id` (keeps latest by `ingestion_timestamp`)

**customers:**
- `dob` → parsed via `parse_date_column()`
- Deduplicates on `customer_id` (keeps latest by `ingestion_timestamp`)

**transactions:**
- `transaction_date` → parsed via `parse_date_column()`
- `amount` → cast to `DECIMAL(18,2)`
- `currency` → standardized to `"ZAR"` (literal)
- `merchant_subcategory` → cast to StringType (handles absent field in Stage 1)
- `dq_flag` → set to `NULL` (Stage 1 data is clean)
- `transaction_timestamp` → combined from `transaction_date` + `transaction_time`
- Deduplicates on `transaction_id` (keeps latest by `transaction_timestamp` then `ingestion_timestamp`)
- Selects specific columns to drop raw fields no longer needed

---

## Layer 3: Gold (`pipeline/provision.py`)

**`run_provisioning()`**

### dim_customers
1. Read Silver customers
2. Generate `customer_sk` = `xxhash64(customer_id)` cast to BIGINT
3. Derive `age_band` from `dob`
4. Select 9 columns per spec

### dim_accounts
1. Read Silver accounts
2. Generate `account_sk` = `xxhash64(account_id)` cast to BIGINT
3. Rename `customer_ref` → `customer_id`
4. **Inner join** with `dim_customers` on `customer_id` — ensures zero unlinked accounts (Query 2)
5. Select 11 columns per spec

### fact_transactions
1. Build `account_lookup` = dim_accounts joined with dim_customers to get `account_sk` + `customer_sk`
2. **Inner join** Silver transactions with `account_lookup` on `account_id`
3. Generate `transaction_sk` = `xxhash64(transaction_id)` cast to BIGINT
4. Extract `province` from `location.province` struct
5. Select 15 columns per spec

**FK integrity:** Inner joins at every stage guarantee:
- Every `dim_accounts.customer_id` → valid `dim_customers.customer_id`
- Every `fact_transactions.account_sk` → valid `dim_accounts.account_sk`
- Every `fact_transactions.customer_sk` → valid `dim_customers.customer_sk`

---

## Docker Setup (`Dockerfile`)

```
FROM nedbank-de-challenge/base:1.0
├── pip install requirements.txt
├── curl delta-spark_2.12-3.1.0.jar → /app/jars/
├── curl delta-storage-3.1.0.jar → /app/jars/
├── COPY pipeline/ config/ validate_gold.py
└── CMD ["python", "pipeline/run_all.py"]
```

**Why pre-download JARs:** Container runs with `--network=none`. Spark's Maven/Ivy resolver would fail at runtime.

**Why uncompressed codec:** Two issues with gzip:
1. Snappy requires extracting native `.so` to `/tmp` which has `noexec` on the scorer's tmpfs
2. Gzip names files `.gz.parquet` — DuckDB's `delta_scan` misinterprets this as whole-file gzip compression

---

## Validation Results

All verified via DuckDB `delta_scan` (exact method the scorer uses):

| Query | Result |
|---|---|
| Q1: Transaction Volume by Type | PASS — 4 types (CREDIT 299,467 / DEBIT 550,201 / FEE 100,313 / REVERSAL 50,019) |
| Q2: Zero Unlinked Accounts | PASS — 0 unlinked |
| Q3: Province Distribution | PASS — 9 provinces (Gauteng 29,794 / Western Cape 18,396 / KwaZulu-Natal 15,327 / ...) |
| SK Uniqueness | PASS — 0 duplicates, 0 nulls across all 3 tables |
| FK Integrity | PASS — 0 orphaned fact rows |
| Currency | PASS — 100% ZAR |
| Schema Conformance | PASS — 15/11/9 fields match spec |

---

## Performance

- **Execution time:** ~4 minutes on 2 vCPU / 2GB (well under 30-min limit)
- **Peak memory:** Under 2GB (no OOM)
- **No anti-patterns:** No `.collect()`, `.toPandas()`, or full-dataset loops

---

## Design Decisions to Defend at Interview

1. **xxhash64 for surrogate keys** — Deterministic, stable across re-runs, no windowing overhead. Trade-off: tiny collision probability vs. `row_number()` which requires a full sort.

2. **Inner joins in Gold** — Drops orphaned accounts/transactions rather than keeping them with NULL FKs. Correct for Query 2 (zero tolerance for unlinked accounts).

3. **Uncompressed parquet** — Larger files on disk but: (a) no native lib extraction needed, (b) DuckDB reads it cleanly via `delta_scan`. Acceptable since disk is not a scored constraint.

4. **Single-pass deduplication via row_number()** — Idiomatic Spark, no `.groupBy().agg()` followed by join pattern. Efficient for Stage 1 where duplicates are rare.

5. **Multi-format date parsing** — `parse_date_column()` coalesces 3 formats. Designed for Stage 2 where date format variants are introduced.

6. **Config-driven paths** — All I/O paths from `pipeline_config.yaml`. No hardcoded paths in pipeline code. Ready for Stage 2/3 path changes.

---

## Local Harness Setup (Windows + WSL2)

The scoring harness (`infrastructure/run_tests.sh`) must run on Linux. On Windows, WSL2 is required because:
- Git Bash mangles Docker volume paths (`/tmp/de_test_output.XXXXXX` → gets MSYS-translated incorrectly)
- The harness creates a temp dir via `mktemp` and mounts it into Docker — this only works reliably in a true Linux shell

### One-time WSL2 Setup

**1. Enable WSL2 + Ubuntu**
```
wsl --install -d Ubuntu          # in PowerShell (Admin)
wsl --set-version Ubuntu 2
```

**2. Enable Docker Desktop WSL2 integration**
- Docker Desktop → Settings → Resources → WSL Integration → enable Ubuntu

**3. Install DuckDB 1.5.2 in WSL Ubuntu**
```bash
# Run inside WSL (as root or with sudo):
curl -L https://github.com/duckdb/duckdb/releases/download/v1.5.2/duckdb_cli-linux-amd64.zip -o /tmp/duckdb.zip
unzip /tmp/duckdb.zip -d /tmp
mv /tmp/duckdb /usr/local/bin/duckdb
chmod +x /usr/local/bin/duckdb
duckdb --version   # should print v1.5.2
```

> **Why 1.5.2 specifically?** The delta extension for DuckDB must be compatible with the Delta Lake 3.x format written by delta-spark 3.1.0. DuckDB 1.2.x has a delta extension that silently fails on these tables.

**4. Fix DuckDB output format for the harness**

DuckDB 1.5.2 defaults to box-drawing table output (e.g. `│ 1000000 │`) even when stdout is piped. The harness uses `grep -E '^[0-9]+$'` and `awk '{print $NF}'` to parse the output — both fail against box-drawing characters.

Fix: create `/root/.duckdbrc` (or `~/.duckdbrc` for non-root users):
```bash
echo '.mode tabs'    > /root/.duckdbrc
echo '.headers off' >> /root/.duckdbrc
```

This switches DuckDB to plain tab-separated output so the harness grep/awk works correctly.

**5. Install delta extension for root**
```bash
duckdb -c "INSTALL delta; LOAD delta; SELECT 'ok';"
```
Run this once to pre-install the extension into root's home (`/root/.duckdb/extensions/`). The harness calls `INSTALL delta` every time but pre-installing avoids any network dependency.

### Running the Harness

From PowerShell (default WSL user is root, so no `-u root` needed):
```powershell
wsl -d Ubuntu -- bash -c "cd /mnt/c/Users/tinashe.chinyati/Downloads/d9e35951823b_stage1/stage1 && bash infrastructure/run_tests.sh --stage 1 --image nedbank-stage1-test --data-dir local_test"
```

Expected runtime: ~4-5 minutes (dominated by the Spark pipeline inside Docker).

### Why Not Run It Directly in PowerShell / Git Bash?

| Shell | Problem |
|---|---|
| PowerShell | Can't run bash scripts natively |
| Git Bash | MSYS path translation mangles `mktemp` temp dirs when passed to Docker `-v` |
| WSL (non-root user) | Docker writes output files as root; WSL user can't read them for DuckDB checks |
| WSL as root | ✅ Works — Docker runs as root, files are readable, duckdbrc is in /root/ |

### Symptoms to Watch For

| Symptom | Cause | Fix |
|---|---|---|
| Check 4: `DuckDB could not read it as a Delta table` | `.duckdbrc` missing → box-drawing output fails grep | Create `/root/.duckdbrc` with `.mode tabs` + `.headers off` |
| Check 5: `Q1 returned 0 rows` | Same output format issue | Same fix |
| Check 4/5 errors about `delta extension not found` | DuckDB version too old (1.2.x) | Upgrade to 1.5.2 |
| Check 2: container exits non-zero | Pipeline error inside Docker | Run container manually: `docker run ... nedbank-stage1-test` and read logs |
| Check 2: files unreadable | Docker wrote as root, WSL user can't read | Run harness as root (`wsl -d Ubuntu -u root`) |
