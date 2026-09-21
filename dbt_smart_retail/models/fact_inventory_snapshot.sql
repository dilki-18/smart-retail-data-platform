select
    product_id,
    store_id as warehouse_id,
    snapshot_date,
    stock_quantity
from raw_warehouse_stock