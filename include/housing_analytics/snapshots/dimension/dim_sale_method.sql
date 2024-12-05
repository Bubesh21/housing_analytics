{% snapshot dim_sale_method %}

{{
    config(
      strategy='check',
      unique_key='sale_method_sk',
      check_cols=['method_code','method_description'],
      invalidate_hard_deletes=True
    )
}}

with source as (
    select distinct 
        method as method_code,
        case 
            when method = 'S' then 'Sale'
            when method = 'SP' then 'Sale by Private Treaty'
            when method = 'PI' then 'Property Insurance'
            when method = 'PN' then 'Private Negotiation'
            when method = 'SN' then 'Sale by Negotiation'
            when method = 'NB' then 'New Build'
            when method = 'VB' then 'Vendor Bid'
            when method = 'W' then 'Withdrawn'
            when method = 'SA' then 'Sale by Auction'
            when method = 'SS' then 'Sale by Set Date'
            when method = 'N/A' then 'Not Available'
            else 'Unknown'
        end as method_description,
        insert_datetime as source_insert_datetime,
        batch_id,
        {{ is_active_ind() }} as dbt_is_active_ind
    from {{ ref('val_housing_price') }}
)

select 
{{ dbt_utils.generate_surrogate_key(['method_code']) }} as sale_method_sk,
* 
from source

{% endsnapshot %}