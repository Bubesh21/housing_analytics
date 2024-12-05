{% macro is_active_ind() %}

    {% if is_incremental() %}
        case when dbt_valid_to is null then 'A' else 'I' end
    {% else %}
        'A'
    {% endif %}

{% endmacro %}
