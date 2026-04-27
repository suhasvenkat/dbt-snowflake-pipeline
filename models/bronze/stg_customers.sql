-- stg_customers: Raw customer staging

with source as (
    select * from {{ source('raw', 'customers') }}
),

renamed as (
    select
        customer_id::varchar       as customer_id,
        first_name::varchar        as first_name,
        last_name::varchar         as last_name,
        email::varchar             as email,
        country::varchar           as country,
        segment::varchar           as customer_segment,
        signup_date::date          as signup_date,
        created_at::timestamp_ntz  as created_at,
        updated_at::timestamp_ntz  as updated_at
    from source
)

select * from renamed
