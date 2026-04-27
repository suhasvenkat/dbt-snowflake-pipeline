-- customer_snapshot: SCD Type 2 history of customer records
-- Every time a customer's data changes, a new row is inserted
-- with dbt_valid_from / dbt_valid_to populated automatically

{% snapshot customer_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select * from {{ ref('int_customers_enriched') }}

{% endsnapshot %}
