from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)
DB = "postgresql+psycopg2://postgres:postgres@retail_postgres:5432/retail"
engine = create_engine(DB)

SQL_CLEAN = """
BEGIN;
SET LOCAL TIME ZONE 'UTC';
TRUNCATE TABLE silver_sales, quarantine_sales;

CREATE TEMP TABLE parsed_sales ON COMMIT DROP AS
SELECT f.*,
    CASE
        WHEN f.timestamp ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?$'
            THEN TO_TIMESTAMP(f.timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.US')
        WHEN f.timestamp ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
            THEN TO_TIMESTAMP(f.timestamp, 'DD-MM-YYYY HH24:MI:SS')
        ELSE NULL
    END AS parsed_ts
FROM fact_sales f;

INSERT INTO silver_sales (transaction_id, product_id, customer_id, store_id,
                          quantity, unit_price, total_amount, sale_ts)
SELECT DISTINCT ON (transaction_id)
    transaction_id, product_id, NULLIF(customer_id, ''), store_id,
    quantity::int, price::numeric(10,2),
    (quantity*price)::numeric(12,2), parsed_ts AT TIME ZONE 'UTC'
FROM parsed_sales
WHERE transaction_id IS NOT NULL AND quantity > 0 AND price > 0
  AND parsed_ts IS NOT NULL AND parsed_ts <= NOW()
ORDER BY transaction_id;

INSERT INTO quarantine_sales (raw_record, error_reason)
SELECT to_jsonb(p), CASE
    WHEN p.transaction_id IS NULL THEN 'missing transaction_id'
    WHEN p.quantity <= 0 THEN 'invalid quantity (<=0)'
    WHEN p.price <= 0 THEN 'invalid price (<=0)'
    WHEN p.parsed_ts IS NULL THEN 'unparseable timestamp'
    WHEN p.parsed_ts > NOW() THEN 'future-dated order'
    ELSE 'unknown' END
FROM parsed_sales p
WHERE p.transaction_id IS NULL OR p.quantity <= 0 OR p.price <= 0
   OR p.parsed_ts IS NULL OR p.parsed_ts > NOW();
COMMIT;
"""

def clean_silver():
    with engine.begin() as conn:
        conn.execute(text(SQL_CLEAN))
    with engine.connect() as conn:
        s = conn.execute(text("SELECT COUNT(*) FROM silver_sales")).scalar()
        q = conn.execute(text("SELECT COUNT(*) FROM quarantine_sales")).scalar()
    logger.info(f"✅ silver_sales={s}  quarantine_sales={q}")

with DAG(
    "week5_silver_cleaning",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["week5","silver","quality"],
    default_args={"retries":1,"retry_delay":timedelta(minutes=1)},
) as dag:
    PythonOperator(task_id="clean_silver_sales", python_callable=clean_silver)