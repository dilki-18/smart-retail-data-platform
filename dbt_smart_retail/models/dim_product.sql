select distinct
    product_id,
    'product_' || product_id::text as product_name,
    'unknown' as category,
    null::text as supplier_id
from raw_warehouse_stock