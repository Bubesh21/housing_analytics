
{{
    config(
      schema='analytics',
      materialized='table'
    )
}}

with property_sales as (
    select 
        {{ dbt_utils.generate_surrogate_key(['address', 'suburb', 'postcode']) }} as property_sk,
        {{ dbt_utils.generate_surrogate_key(['type']) }} as property_type_sk,
        {{ dbt_utils.generate_surrogate_key(['method']) }} as sale_method_sk,
        {{ dbt_utils.generate_surrogate_key(['suburb', 'postcode']) }} as location_sk,
        date as sale_date,
        price as sale_price,
        case 
            when buildingarea > 0 then 
                round(price / nullif(buildingarea, 0), 2)
            else null 
        end as price_per_built_sqm,
        case 
            when landsize > 0 then price / nullif(landsize, 0)
            else null 
        end as price_per_land_sqm,
        insert_datetime,
        batch_id
    from {{ ref('val_housing_price') }}
),

final as (
    select 
        {{ dbt_utils.generate_surrogate_key(['property_sk', 'sale_date']) }} as sale_sk,
        property_sk,
        property_type_sk,
        sale_method_sk,
        location_sk,
        sale_date,
        extract(year from sale_date) as sale_year,
        extract(month from sale_date) as sale_month,
        extract(quarter from sale_date) as sale_quarter,
        sale_price,
        price_per_built_sqm,
        price_per_land_sqm,
        insert_datetime as source_insert_datetime,
        batch_id,
        current_timestamp() as dbt_created_at
    from property_sales
)

select * from final