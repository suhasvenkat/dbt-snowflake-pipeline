-- stg_products: Raw product staging

with source as (
    select * from {{ source('raw', 'products') }}
),

renamed as (
    select
        product_id::varchar        as product_id,
        product_name::varchar      as product_name,
        category::varchar          as category,
        subcategory::varchar       as subcategory,
        unit_cost::float           as unit_cost,
        is_active::boolean         as is_active,
        created_at::timestamp_ntz  as created_at,
        updated_at::timestamp_ntz  as updated_at
    from source
)

select * from renamed
