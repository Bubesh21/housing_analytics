{{ config(schema='access', materialized='view') }}

with source as (
    select 
        e.bedrooms,
        e.bathrooms,
        e.car_spaces,
        d.type_description as property_type,
        d.type_category,
        count(*) as total_properties,
        avg(e.land_size_sqm) as avg_land_size,
        avg(e.building_area_sqm) as avg_building_area,
        round(avg(e.building_coverage_ratio), 2) as avg_building_coverage_ratio,
        avg(sale_year - year_built) as avg_property_age,
        min(sale_year - year_built) as youngest_property,
        max(sale_year - year_built) as oldest_property,
        avg(sale_price) as avg_sale_price,
        min(sale_price) as min_sale_price,
        max(sale_price) as max_sale_price,
        round(avg(case 
            when building_area_sqm > 0 then sale_price / building_area_sqm 
            else null 
        end), 2) as avg_price_per_sqm,
        count(distinct b.suburb) as suburbs_present,
        count(distinct b.region_name) as regions_present,
        a.batch_id
    from {{ ref('fact_property_sales') }} a

    inner join {{ ref('dim_location') }} b
        on a.location_sk = b.location_sk
        and b.dbt_is_active_ind = 'A'

    inner join {{ ref('dim_sale_method') }} c
        on a.sale_method_sk = c.sale_method_sk
        and c.dbt_is_active_ind = 'A'

    inner join {{ ref('dim_property_type') }} d
        on a.property_type_sk = d.property_type_sk
        and d.dbt_is_active_ind = 'A'

    inner join {{ ref('dim_property') }} e
        on a.property_sk = e.property_sk
        and e.dbt_is_active_ind = 'A'

    group by 
        e.bedrooms,
        e.bathrooms,
        e.car_spaces,
        d.type_description,
        d.type_category,
        a.batch_id
)

select * from source