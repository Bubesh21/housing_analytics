{{ config(schema='access', materialized='view') }}

with source as (
    select 
        sale_month,
        b.suburb,
        d.type_description as property_type,
        count(*) as monthly_sales_count,
        avg(sale_price) as avg_monthly_price,
        median(sale_price) as median_monthly_price,
        a.batch_id
    from {{ ref('fact_property_sales') }} a
    inner join {{ ref('dim_location') }} b
        on a.location_sk = b.location_sk 
        and b.dbt_is_active_ind = 'A'
    inner join {{ ref('dim_property_type') }} d
        on a.property_type_sk = d.property_type_sk 
        and d.dbt_is_active_ind = 'A'
    inner join {{ ref('dim_property') }} e
        on a.property_sk = e.property_sk 
        and e.dbt_is_active_ind = 'A'
    group by 
        sale_month,
        b.suburb,
        d.type_description,
        a.batch_id
),

moving_averages as (
    select 
        *,
        avg(avg_monthly_price) over (
            partition by suburb, property_type 
            order by sale_month 
            rows between 2 preceding and current row
        ) as price_ma_3m,
        
        avg(monthly_sales_count) over (
            partition by suburb, property_type 
            order by sale_month 
            rows between 2 preceding and current row
        ) as volume_ma_3m,
        
        avg(avg_monthly_price) over (
            partition by suburb, property_type 
            order by sale_month 
            rows between 5 preceding and current row
        ) as price_ma_6m,
        
        avg(monthly_sales_count) over (
            partition by suburb, property_type 
            order by sale_month 
            rows between 5 preceding and current row
        ) as volume_ma_6m,
    
        avg(avg_monthly_price) over (
            partition by suburb, property_type 
            order by sale_month 
            rows between 11 preceding and current row
        ) as price_ma_12m,
        
        avg(monthly_sales_count) over (
            partition by suburb, property_type 
            order by sale_month 
            rows between 11 preceding and current row
        ) as volume_ma_12m,
    from source
)

select 
    sale_month,
    suburb,
    property_type,
    monthly_sales_count,
    avg_monthly_price,
    median_monthly_price,
    round(price_ma_3m, 2) as price_ma_3m,
    round(volume_ma_3m, 1) as volume_ma_3m,
    round(price_ma_6m, 2) as price_ma_6m,
    round(volume_ma_6m, 1) as volume_ma_6m,
    round(price_ma_12m, 2) as price_ma_12m,
    round(volume_ma_12m, 1) as volume_ma_12m,
    case 
        when price_ma_3m > price_ma_6m and price_ma_6m > price_ma_12m then 'Strong Uptrend'
        when price_ma_3m < price_ma_6m and price_ma_6m < price_ma_12m then 'Strong Downtrend'
        when price_ma_3m > price_ma_6m then 'Weak Uptrend'
        when price_ma_3m < price_ma_6m then 'Weak Downtrend'
        else 'Neutral'
    end as price_trend,
    case 
        when volume_ma_3m > volume_ma_6m then 'Increasing Volume'
        when volume_ma_3m < volume_ma_6m then 'Decreasing Volume'
        else 'Stable Volume'
    end as volume_trend,
    batch_id
from moving_averages
order by 
    suburb,
    property_type,
    sale_month