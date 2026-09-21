select
    customer_id,
    name as customer_name,
    email as hashed_email,
    loyalty_tier,
    region
from raw_crm_customers