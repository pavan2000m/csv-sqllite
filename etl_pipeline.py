"""
CSV → SQLite ETL Pipeline
=========================
Extracts data from a CSV file, validates and transforms it,
then loads it into a SQLite database with aggregated analytics tables.

Author: Pavan Kalyan Reddy
"""

import csv
import sqlite3
import os
import logging
from datetime import datetime, timezone

def now_utc():
    """Return current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)
from collections import defaultdict

# ── Configuration ────────────────────────────────────────────────────────────
CSV_FILE   = "sales_data.csv"
DB_FILE    = "sales_warehouse.db"
LOG_FILE   = "etl_pipeline.log"

REQUIRED_COLUMNS = {"order_id", "customer_name", "email", "product",
                    "category", "quantity", "unit_price", "order_date", "region"}

# ── Logging Setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# EXTRACT
# ══════════════════════════════════════════════════════════════════════════════
def extract(filepath: str) -> list[dict]:
    """Read raw rows from CSV and return as a list of dicts."""
    log.info(f"[EXTRACT] Reading file: {filepath}")

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"CSV file not found: {filepath}")

    rows = []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Schema validation
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing columns in CSV: {missing}")

        for row in reader:
            rows.append(dict(row))

    log.info(f"[EXTRACT] Loaded {len(rows)} raw rows.")
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATE & TRANSFORM
# ══════════════════════════════════════════════════════════════════════════════
def transform(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Clean, validate, and enrich each row.
    Returns (valid_rows, rejected_rows).
    """
    log.info("[TRANSFORM] Starting validation and transformation...")

    valid, rejected = [], []

    for row in rows:
        errors = []

        # ── Type coercion ────────────────────────────────────────────────────
        try:
            row["order_id"]   = int(row["order_id"])
            row["quantity"]   = int(row["quantity"])
            row["unit_price"] = float(row["unit_price"])
        except (ValueError, KeyError) as e:
            errors.append(f"Type error: {e}")

        # ── Business rules ───────────────────────────────────────────────────
        if not errors:
            if row["quantity"] <= 0:
                errors.append(f"Invalid quantity: {row['quantity']}")
            if row["unit_price"] <= 0:
                errors.append(f"Invalid unit_price: {row['unit_price']}")
            if not row.get("customer_name", "").strip():
                errors.append("Missing customer_name")

        # ── Date validation ──────────────────────────────────────────────────
        if not errors:
            try:
                datetime.strptime(row["order_date"], "%Y-%m-%d")
            except ValueError:
                errors.append(f"Bad date format: {row['order_date']}")

        # ── Reject or enrich ─────────────────────────────────────────────────
        if errors:
            row["_errors"] = "; ".join(errors)
            rejected.append(row)
        else:
            # Derived fields
            row["total_price"]  = round(row["quantity"] * row["unit_price"], 2)
            row["email"]        = row["email"].strip() if row["email"].strip() else None
            row["customer_name"] = row["customer_name"].strip().title()
            row["ingested_at"]  = now_utc().strftime("%Y-%m-%d %H:%M:%S")
            valid.append(row)

    log.info(f"[TRANSFORM] Valid: {len(valid)} | Rejected: {len(rejected)}")
    return valid, rejected


# ══════════════════════════════════════════════════════════════════════════════
# LOAD
# ══════════════════════════════════════════════════════════════════════════════
def load(valid_rows: list[dict], rejected_rows: list[dict], db_path: str):
    """Load valid rows into SQLite; persist rejected rows for auditing."""
    log.info(f"[LOAD] Connecting to database: {db_path}")

    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    # ── DDL: orders fact table ────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id      INTEGER PRIMARY KEY,
            customer_name TEXT    NOT NULL,
            email         TEXT,
            product       TEXT    NOT NULL,
            category      TEXT    NOT NULL,
            quantity      INTEGER NOT NULL,
            unit_price    REAL    NOT NULL,
            total_price   REAL    NOT NULL,
            order_date    TEXT    NOT NULL,
            region        TEXT    NOT NULL,
            ingested_at   TEXT    NOT NULL
        )
    """)

    # ── DDL: rejected rows audit table ───────────────────────────────────────
    cur.execute("DELETE FROM rejected_records") if cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='rejected_records'"
    ).fetchone() else None
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rejected_records (
            order_id      TEXT,
            customer_name TEXT,
            errors        TEXT,
            raw_row       TEXT,
            rejected_at   TEXT
        )
    """)

    # ── Insert valid rows (upsert) ────────────────────────────────────────────
    insert_sql = """
        INSERT OR REPLACE INTO orders
            (order_id, customer_name, email, product, category,
             quantity, unit_price, total_price, order_date, region, ingested_at)
        VALUES
            (:order_id, :customer_name, :email, :product, :category,
             :quantity, :unit_price, :total_price, :order_date, :region, :ingested_at)
    """
    cur.executemany(insert_sql, valid_rows)
    log.info(f"[LOAD] Inserted {cur.rowcount} rows into `orders`.")

    # ── Insert rejected rows ──────────────────────────────────────────────────
    now = now_utc().strftime("%Y-%m-%d %H:%M:%S")
    for r in rejected_rows:
        cur.execute("""
            INSERT INTO rejected_records (order_id, customer_name, errors, raw_row, rejected_at)
            VALUES (?, ?, ?, ?, ?)
        """, (r.get("order_id"), r.get("customer_name"), r.get("_errors"), str(r), now))

    # ── Aggregation: daily sales by region ───────────────────────────────────
    cur.execute("DROP TABLE IF EXISTS daily_sales_by_region")
    cur.execute("""
        CREATE TABLE daily_sales_by_region AS
        SELECT
            order_date,
            region,
            COUNT(*)            AS total_orders,
            SUM(total_price)    AS total_revenue,
            AVG(total_price)    AS avg_order_value
        FROM orders
        GROUP BY order_date, region
        ORDER BY order_date, region
    """)

    # ── Aggregation: category performance ────────────────────────────────────
    cur.execute("DROP TABLE IF EXISTS category_performance")
    cur.execute("""
        CREATE TABLE category_performance AS
        SELECT
            category,
            COUNT(*)            AS total_orders,
            SUM(quantity)       AS units_sold,
            SUM(total_price)    AS total_revenue,
            ROUND(AVG(unit_price), 2) AS avg_unit_price
        FROM orders
        GROUP BY category
        ORDER BY total_revenue DESC
    """)

    conn.commit()
    conn.close()
    log.info("[LOAD] Database committed and closed.")


# ══════════════════════════════════════════════════════════════════════════════
# REPORT
# ══════════════════════════════════════════════════════════════════════════════
def report(db_path: str):
    """Print a summary report from the loaded data."""
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    print("\n" + "═" * 55)
    print("  ETL PIPELINE — SUMMARY REPORT")
    print("═" * 55)

    cur.execute("SELECT COUNT(*), SUM(total_price) FROM orders")
    total_orders, total_revenue = cur.fetchone()
    print(f"\n  Total Orders Loaded : {total_orders}")
    print(f"  Total Revenue       : ${total_revenue:,.2f}")

    cur.execute("SELECT COUNT(*) FROM rejected_records")
    rejected = cur.fetchone()[0]
    print(f"  Rejected Records    : {rejected}")

    print("\n  ── Revenue by Category ──────────────────────────")
    cur.execute("SELECT category, total_orders, units_sold, total_revenue FROM category_performance")
    for cat, orders, units, rev in cur.fetchall():
        print(f"  {cat:<15} | {orders:>3} orders | {units:>4} units | ${rev:>9,.2f}")

    print("\n  ── Top 5 Regions by Revenue ─────────────────────")
    cur.execute("""
        SELECT region, SUM(total_price) AS rev
        FROM orders GROUP BY region ORDER BY rev DESC LIMIT 5
    """)
    for region, rev in cur.fetchall():
        print(f"  {region:<12} | ${rev:>9,.2f}")

    if rejected > 0:
        print("\n  ── Rejected Records ─────────────────────────────")
        cur.execute("SELECT order_id, customer_name, errors FROM rejected_records")
        for oid, name, err in cur.fetchall():
            print(f"  Order {oid} ({name}): {err}")

    print("\n" + "═" * 55)
    conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def run_pipeline():
    log.info("=" * 50)
    log.info("ETL PIPELINE STARTED")
    log.info("=" * 50)

    start = now_utc()

    raw_rows              = extract(CSV_FILE)
    valid_rows, rejected  = transform(raw_rows)
    load(valid_rows, rejected, DB_FILE)
    report(DB_FILE)

    elapsed = (now_utc() - start).total_seconds()
    log.info(f"ETL PIPELINE COMPLETED in {elapsed:.2f}s")


if __name__ == "__main__":
    run_pipeline()
