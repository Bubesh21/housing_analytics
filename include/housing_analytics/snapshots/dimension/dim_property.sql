{% snapshot dim_property %}

{{
    config(
      strategy='check',
      unique_key='property_sk',
      check_cols=['rooms', 'property_type_code', 'bedrooms', 'bathrooms', 'car_spaces', 'land_size_sqm', 'building_area_sqm', 'year_built'],
      invalidate_hard_deletes=True
    )
}}

with source as (
    select
        address,
        suburb,
        postcode,
        max(rooms) rooms,
        max(type) type,
        max(bedroom2) bedroom2,
        max(bathroom) bathroom,
        max(car) car,
        max(landsize) landsize,
        max(buildingarea) buildingarea,
        max(yearbuilt) yearbuilt,
        max(lattitude) lattitude,
        max(longitude) longitude,
        case 
            when max(landsize) > 0 and max(buildingarea) > 0 
            then round(max(buildingarea) / max(landsize) * 100, 2)
            else null 
        end as building_coverage_ratio,
        max(distance) as distance_from_cbd,
        max(insert_datetime) as source_insert_datetime,
        max(batch_id) as batch_id
    from {{ ref('val_housing_price') }}
    group by 
        address,
        suburb,
        postcode
),

validated as (
    select
        {{ dbt_utils.generate_surrogate_key(['address', 'suburb', 'postcode']) }} as property_sk,
        address,
        suburb,
        postcode,
        case 
            when rooms < 0 then null 
            else rooms 
        end as rooms,
        type as property_type_code,
        case 
            when bedroom2 < 0 then null 
            else bedroom2 
        end as bedrooms,
        case 
            when bathroom < 0 then null 
            else bathroom 
        end as bathrooms,
        case 
            when car < 0 then null 
            else car 
        end as car_spaces,
        case 
            when distance_from_cbd <= 5 then 'Inner City'
            when distance_from_cbd <= 10 then 'Inner Suburbs'
            when distance_from_cbd <= 20 then 'Middle Suburbs'
            when distance_from_cbd > 20 then 'Outer Suburbs'
            else 'Unknown'
        end as geographic_zone,
        lattitude,
        longitude,
        case 
            when landsize < 0 then null 
            else landsize 
        end as land_size_sqm,
        case 
            when buildingarea < 0 then null 
            else buildingarea 
        end as building_area_sqm,
        case 
            when yearbuilt < 1800 or yearbuilt > extract(year from current_date()) then null 
            else yearbuilt 
        end as year_built,
        building_coverage_ratio,
        source_insert_datetime,
        batch_id,
        {{ is_active_ind() }} as dbt_is_active_ind
    from source
)

select * from validated

{% endsnapshot %}