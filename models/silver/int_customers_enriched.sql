-- int_customers_enriched: Customer with derived segments and order history

with customers as (
    select * from {{ ref('stg_customers') }}
),

order_summary as (
    select
        customer_id,
        count(distinct order_id)         as total_orders,
        sum(total_amount)                as lifetime_value,
        min(order_date)                  as first_order_date,
        max(order_date)                  as last_order_date,
        -- Days since last order
        datediff('day', max(order_date), current_date()) as days_since_last_order
    from {{ ref('int_orders_cleaned') }}
    where order_status = 'completed'
    group by 1
),

enriched as (
    select
        c.*,
        coalesce(o.total_orders, 0)      as total_orders,
        coalesce(o.lifetime_value, 0)    as lifetime_value,
        o.first_order_date,
        o.last_order_date,
        o.days_since_last_order,
        -- RFM-based value segment
        case
            when o.lifetime_value >= 10000 then 'high_value'
            when o.lifetime_value >= 1000  then 'mid_value'
            when o.lifetime_value > 0      then 'low_value'
            else 'no_purchases'
        end                              as value_segment,
        -- Churn risk flag
        case
            when o.days_since_last_order > 180 then true
            else false
        end                              as is_churn_risk
    from customers c
    left join order_summary o on c.customer_id = o.customer_id
)

select * from enriched
