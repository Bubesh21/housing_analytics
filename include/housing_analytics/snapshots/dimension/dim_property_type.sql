{% snapshot dim_property_type %}

{{
    config(
      strategy='check',
      unique_key='property_type_sk',
      check_cols=['type_code','type_description', 'type_category', 'type_subcategory'],
      invalidate_hard_deletes=True
    )
}}

with source as (
    select distinct 
        type as type_code,
        case 
            when type = 'br' then 'Bedroom(s)'
            when type = 'h' then 'House'
            when type = 'u' then 'Unit'
            when type = 't' then 'Townhouse'
            when type = 'dev site' then 'Development Site'
            when type = 'o res' then 'Other Residential'
            else 'Unknown'
        end as type_description,
        case 
            when type in ('h', 'u', 't') then 'Residential Building'
            when type = 'br' then 'Room'
            when type = 'dev site' then 'Development'
            when type = 'o res' then 'Other'
            else 'Unknown'
        end as type_category,
        case 
            when type = 'h' then 'House/Cottage/Villa/Semi/Terrace'
            when type = 'u' then 'Unit/Duplex'
            when type = 't' then 'Townhouse'
            when type = 'br' then 'Bedroom'
            when type = 'dev site' then 'Development Site'
            when type = 'o res' then 'Other Residential Type'
            else 'Unknown'
        end as type_subcategory,
        insert_datetime as source_insert_datetime,
        batch_id,
        {{ is_active_ind() }} as dbt_is_active_ind
    from {{ ref('val_housing_price') }}
)

select {{ dbt_utils.generate_surrogate_key(['type_code']) }} as property_type_sk,* from source

{% endsnapshot %}