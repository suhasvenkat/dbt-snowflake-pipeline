-- fct_revenue: Revenue fact table (incremental)
-- Only processes records since last run — efficient for large datasets

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='sync_all_columns'
    )
}}

with orders as (
    select * from {{ ref('int_orders_cleaned') }}
    {% if is_incremental() %}
        -- Only process new/updated records
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

customers as (
    select customer_id, country, customer_segment, value_segment
    from {{ ref('dim_customer') }}
),

products as (
    select product_id, category, subcategory
    from {{ ref('dim_product') }}
),

final as (
    select
        -- Keys
        o.order_id,
        o.customer_id,
        o.product_id,
        -- Dates
        o.order_date,
        date_trunc('month', o.order_date)   as order_month,
        date_trunc('quarter', o.order_date) as order_quarter,
        year(o.order_date)                  as order_year,
        -- Status
        o.order_status,
        -- Dimensions (denormalized for BI performance)
        c.country,
        c.customer_segment,
        c.value_segment,
        p.category                          as product_category,
        p.subcategory                       as product_subcategory,
        -- Measures
        o.quantity,
        o.unit_price,
        o.unit_cost,
        o.total_amount                      as gross_revenue,
        o.gross_margin,
        round(o.gross_margin / nullif(o.total_amount, 0) * 100, 2) as margin_pct,
        -- Audit
        o.created_at,
        o.updated_at,
        current_timestamp()                 as dbt_updated_at
    from orders o
    left join customers c on o.customer_id = c.customer_id
    left join products p on o.product_id = p.product_id
    where o.order_status = 'completed'
)

select * from final
