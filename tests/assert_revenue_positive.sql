-- Custom singular test: all completed order revenue must be positive
-- This test FAILS (returns rows) if any completed orders have zero or negative revenue

select
    order_id,
    gross_revenue
from {{ ref('fct_revenue') }}
where
    order_status = 'completed'
    and gross_revenue <= 0
