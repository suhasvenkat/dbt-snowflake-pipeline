-- dim_product: Product dimension (active products only)

with products as (
    select * from {{ ref('stg_products') }}
)

select
    product_id,
    product_name,
    category,
    subcategory,
    unit_cost,
    is_active,
    created_at,
    updated_at,
    current_timestamp() as dbt_updated_at
from products
where is_active = true
