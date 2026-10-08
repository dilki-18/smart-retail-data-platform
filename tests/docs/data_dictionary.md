# RetailLake — Gold Layer Data Dictionary

## Fact Tables

### fact_sales
- Grain: One row per sales line item
- Primary key: transaction_id (BIGINT)
- Rows: 4,999
- Source: silver_sales

### fact_inventory_snapshot
- Grain: One row per product × warehouse × day
- Primary key: (snapshot_date, product_id, warehouse_id)
- Rows: 73,970

## Dimension Tables

| Table | PK | Rows | Notes |
|-------|-----|------|-------|
| dim_customers | customer_id (TEXT) | 3,190 | Email SHA-256 hashed |
| dim_products | product_id (BIGINT) | 300 | 63 placeholders for orphan sales |
| dim_store | store_id (BIGINT) | 10 | Synthetic |
| dim_suppliers | supplier_id (TEXT) | 500 | — |
| dim_warehouse | warehouse_id (BIGINT) | 10 | — |
| dim_date | date_key (INT) | 1,461 | 2022-01-01 → 2025-12-31 |

## Layers

| Layer | Storage | Purpose |
|-------|---------|---------|
| Raw | MinIO | Unchanged source files |
| Silver | Postgres `silver_*` | Cleaned, deduped, PII hashed |
| Quarantine | Postgres `quarantine_*` | Bad rows with reasons |
| Gold | Postgres `fact_*`, `dim_*` | Dashboard-ready |

## Quality Tests — All Pass

1. No duplicate customer_ids
2. No duplicate transaction_ids
3. No null customer_ids
4. No negative inventory
5. All fact_sales.product_id → dim_products
6. All fact_sales.store_id → dim_store
7. All inventory.product_id → dim_products
8. All inventory.warehouse_id → dim_warehouse