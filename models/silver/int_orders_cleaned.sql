-- int_orders_cleaned: Business logic applied
-- Removes test orders, normalizes statuses, calculates margin

with orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

cleaned as (
    select
        o.order_id,
        o.customer_id,
        o.product_id,
        o.order_date,
        -- Normalize order status values
        case o.order_status
            when 'complete'   then 'completed'
            when 'done'       then 'completed'
            when 'cancelled'  then 'cancelled'
            when 'canceled'   then 'cancelled'
            else lower(o.order_status)
        end                        as order_status,
        o.quantity,
        o.unit_price,
        o.total_amount,
        p.unit_cost,
        -- Calculate gross margin
        (o.unit_price - p.unit_cost) * o.quantity as gross_margin,
        o.created_at,
        o.updated_at
    from orders o
    left join products p on o.product_id = p.product_id
    where
        -- Exclude test orders
        o.order_id not like 'TEST%'
        -- Exclude future-dated orders (data quality guard)
        and o.order_date <= current_date()
        -- Exclude zero-value orders
        and o.total_amount > 0
)

select * from cleaned
