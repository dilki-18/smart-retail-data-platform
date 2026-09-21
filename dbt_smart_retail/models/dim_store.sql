select distinct
    store_id,
    'store_' || store_id::text as store_name,
    'unknown' as region
from raw_pos_sales