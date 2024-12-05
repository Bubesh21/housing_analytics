{{ config(target_schema='access',materialized='view') }}

with source as (
    select 
        b.suburb, 
        b.postcode, 
        b.region_name,
        b.council_area,
        method_description,
        count(*) as total_properties,
        sum(case when type_code = 'h' then 1 else 0 end) as total_houses,
        sum(case when type_code = 'u' then 1 else 0 end) as total_units,
        sum(case when type_code = 't' then 1 else 0 end) as total_townhouses,
        avg(land_size_sqm) as avg_land_size,
        avg(building_area_sqm) as avg_building_area,
        avg(sale_year - year_built) avg_property_age,
        avg(sale_price) as avg_sale_price,
        min(sale_price) as min_sale_price,
        max(sale_price) as max_sale_price,
        a.batch_id
    from {{ ref('fact_property_sales') }} a

    inner join {{ ref('dim_location') }} b
    on a.location_sk = b.location_sk
    and b.dbt_is_active_ind ='A'

    inner join {{ ref('dim_sale_method') }} c
    on a.sale_method_sk = c.sale_method_sk
    and c.dbt_is_active_ind = 'A'

    inner join {{ ref('dim_property_type') }} d
    on a.property_type_sk = d.property_type_sk
    and d.dbt_is_active_ind = 'A'

    inner join {{ ref('dim_property') }} e
    on a.property_sk = e.property_sk
    and e.dbt_is_active_ind ='A'

    group by b.suburb, b.postcode, b.region_name,b.council_area,method_description,a.batch_id
)

select * from source