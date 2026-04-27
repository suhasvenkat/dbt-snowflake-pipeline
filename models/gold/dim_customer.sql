-- dim_customer: Customer dimension (current state)
-- For full history use customer_snapshot

with customers as (
    select * from {{ ref('int_customers_enriched') }}
)

select
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name  as full_name,
    email,
    country,
    customer_segment,
    value_segment,
    is_churn_risk,
    signup_date,
    total_orders,
    lifetime_value,
    first_order_date,
    last_order_date,
    days_since_last_order,
    created_at,
    updated_at,
    -- Audit fields
    current_timestamp()             as dbt_updated_at
from customers
