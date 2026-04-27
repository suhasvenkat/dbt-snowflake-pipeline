-- stg_orders: Raw orders staging — rename cols, cast types, no business logic
-- Source: AWS S3 external stage loaded into Snowflake raw schema

with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        order_id::varchar          as order_id,
        customer_id::varchar       as customer_id,
        product_id::varchar        as product_id,
        order_date::date           as order_date,
        order_status::varchar      as order_status,
        quantity::integer          as quantity,
        unit_price::float          as unit_price,
        total_amount::float        as total_amount,
        created_at::timestamp_ntz  as created_at,
        updated_at::timestamp_ntz  as updated_at
    from source
)

select * from renamed
