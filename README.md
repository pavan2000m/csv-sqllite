# CSV → SQLite ETL Pipeline

A production-style ETL (Extract, Transform, Load) pipeline built in Python that ingests raw CSV sales data, validates and transforms it, and loads it into a structured SQLite data warehouse with aggregated analytics tables.

## Project Structure

```
etl_pipeline/
├── data/
│   └── sales_data.csv       # Raw source data (25 records)
├── etl_pipeline.py          # Main ETL script
├── sales_warehouse.db       # Output: SQLite data warehouse (auto-generated)
├── etl_pipeline.log         # Pipeline run logs (auto-generated)
└── README.md
```

## What It Does

### Extract
- Reads raw CSV data from the `data/` directory
- Validates that all required columns are present (schema enforcement)
- Loads 25 raw records into memory

### Transform
- **Type coercion:** Converts strings to correct types (int, float)
- **Business rule validation:**
  - Rejects rows with `quantity <= 0`
  - Rejects rows with `unit_price <= 0`
  - Flags missing customer names
  - Validates date format (YYYY-MM-DD)
- **Data enrichment:**
  - Computes `total_price = quantity × unit_price`
  - Normalizes customer names to Title Case
  - Adds `ingested_at` timestamp
- Result: **23 valid rows**, **2 rejected rows** routed to audit table

### Load
- Loads valid records into the `orders` fact table (upsert logic)
- Persists rejected records to `rejected_records` audit table
- Builds two aggregated analytics tables:
  - `daily_sales_by_region` — revenue and order counts per day/region
  - `category_performance` — units sold, revenue, avg price per category

## How to Run

```bash
# No external dependencies required — uses Python standard library only
python etl_pipeline.py
```

## Sample Output

```
═══════════════════════════════════════════════════════
  ETL PIPELINE — SUMMARY REPORT
═══════════════════════════════════════════════════════

  Total Orders Loaded : 23
  Total Revenue       : $9,104.47
  Rejected Records    : 2

  ── Revenue by Category ──────────────────────────
  Electronics     |  11 orders |   17 units | $ 6,889.83
  Furniture       |   4 orders |    5 units | $ 1,499.95
  Appliances      |   5 orders |    8 units | $   599.92
  Stationery      |   3 orders |   23 units | $   114.77

  ── Top 5 Regions by Revenue ─────────────────────
  North        | $ 2,949.83
  South        | $ 2,469.82
  East         | $ 1,949.93
  West         | $ 1,734.89

  ── Rejected Records ─────────────────────────────
  Order 1006 (Frank Miller): Invalid quantity: -1
  Order 1021 (Uma Lee): Invalid quantity: 0
```

## Key Data Engineering Concepts Demonstrated

| Concept | Implementation |
|---|---|
| Schema validation | Checks required columns before processing |
| Data quality checks | Type validation, business rules, null handling |
| ETL separation | Clear Extract → Transform → Load stages |
| Audit logging | Rejected records table + pipeline log file |
| Upsert logic | `INSERT OR REPLACE` prevents duplicate loads |
| Aggregation tables | Pre-computed analytics for BI consumption |
| Structured logging | Timestamped logs to both console and file |

## Technologies Used
- **Python 3** (standard library only — `csv`, `sqlite3`, `logging`, `datetime`)
- **SQLite** as the data warehouse target
