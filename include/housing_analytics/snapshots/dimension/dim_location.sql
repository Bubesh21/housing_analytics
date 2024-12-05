{% snapshot dim_location %}

{{
    config(
      strategy='check',
      unique_key='location_sk',
      check_cols=['suburb', 'postcode', 'council_area', 'region_name','property_count'],
      invalidate_hard_deletes=True
    )
}}

with source as (
    select 
        suburb,
        postcode,
        max(councilarea) council_area,
        max(regionname) region_name,
        max(propertycount) property_count,
        max(insert_datetime) insert_datetime,
        max(batch_id) batch_id
    from {{ ref('val_housing_price') }}
    group by suburb, postcode
),

validated as (
    select 
        {{ dbt_utils.generate_surrogate_key(['suburb', 'postcode']) }} as location_sk,
        suburb,
        postcode,
        council_area,
        region_name,
        case
            when property_count < 0 then null
            else property_count
        end as property_count,
        insert_datetime as source_insert_datetime,
        batch_id,
        {{ is_active_ind() }} as dbt_is_active_ind
    from source
)

select * from validated

{% endsnapshot %}