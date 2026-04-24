"""
Run the three scoring validation queries against the Gold layer output.
Usage (inside container):  python validate_gold.py
"""
import sys
import duckdb

GOLD = "/data/output/gold"

def scan(table: str) -> str:
    """Return a DuckDB read_parquet expression for a Delta table directory."""
    return f"read_parquet('{GOLD}/{table}/**/*.parquet', hive_partitioning=false)"

con = duckdb.connect()

print("\n=== QUERY 1: Transaction Volume by Type ===")
q1 = con.execute(f"""
    SELECT
        transaction_type,
        COUNT(*)                        AS record_count,
        ROUND(SUM(amount), 2)           AS total_amount,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
    FROM {scan('fact_transactions')}
    GROUP BY transaction_type
    ORDER BY transaction_type
""").fetchall()
if len(q1) == 4:
    print("  PASS — 4 transaction types found")
else:
    print(f"  FAIL — expected 4 rows, got {len(q1)}")
for row in q1:
    print(f"  {row}")

print("\n=== QUERY 2: Zero Unlinked Accounts (ZERO TOLERANCE) ===")
q2 = con.execute(f"""
    SELECT COUNT(*) AS unlinked_accounts
    FROM {scan('dim_accounts')} AS a
    LEFT JOIN {scan('dim_customers')} AS c
           ON a.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
""").fetchone()[0]
if q2 == 0:
    print("  PASS — 0 unlinked accounts")
else:
    print(f"  FAIL — {q2} unlinked accounts (must be 0)")

print("\n=== QUERY 3: Province Distribution (9 SA provinces) ===")
q3 = con.execute(f"""
    SELECT c.province, COUNT(DISTINCT a.account_id) AS account_count
    FROM {scan('dim_accounts')} AS a
    JOIN {scan('dim_customers')} AS c
      ON a.customer_id = c.customer_id
    GROUP BY c.province
    ORDER BY c.province
""").fetchall()
expected_provinces = {
    "Eastern Cape", "Free State", "Gauteng", "KwaZulu-Natal",
    "Limpopo", "Mpumalanga", "North West", "Northern Cape", "Western Cape",
}
found_provinces = {row[0] for row in q3}
missing = expected_provinces - found_provinces
if len(q3) == 9 and not missing:
    print("  PASS — all 9 provinces present")
else:
    print(f"  FAIL — got {len(q3)} provinces, missing: {missing or 'none'}")
for row in q3:
    print(f"  {row}")

print()
fails = sum([len(q1) != 4, q2 != 0, len(q3) != 9])
sys.exit(1 if fails else 0)
