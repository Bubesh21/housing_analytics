{{ config(schema='access', materialized='view') }}

with source as (
    select 
        b.suburb,
        b.postcode,
        b.region_name,
        e.bedrooms,
        e.bathrooms,
        e.car_spaces,
        d.type_description as property_type,
        count(*) as total_properties,
        avg(sale_price) as avg_sale_price,
        min(sale_price) as min_sale_price,
        max(sale_price) as max_sale_price,
        percentile_cont(0.5) within group (order by sale_price) as median_sale_price,
        avg(e.building_area_sqm) as avg_building_area,
        round(avg(case 
            when e.building_area_sqm > 0 then sale_price / e.building_area_sqm 
            else null 
        end), 2) as avg_price_per_sqm,
        min(sale_year) as earliest_sale,
        max(sale_year) as latest_sale,
        sum(case 
            when months_between(current_date(), sale_date) <= 12 then 1 
            else 0 
        end) as sales_last_12_months,
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
        b.suburb,
        b.postcode,
        b.region_name,
        e.bedrooms,
        e.bathrooms,
        e.car_spaces,
        d.type_description,
        a.batch_id
)

select 
    *,
    round(avg_sale_price - avg(avg_sale_price) over (partition by suburb), 2) as price_diff_from_suburb_avg,
    dense_rank() over (order by avg_sale_price desc) as price_ranking
from source
order by 
    suburb,
    bedrooms,
    bathrooms,
    car_spaces,
    property_type