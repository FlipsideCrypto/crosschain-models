{% macro run_sp_bulk_get_coin_gecko_asset_metadata() %}
    {% set sql %}
        call silver.sp_bulk_get_coin_gecko_asset_metadata();
    {% endset %}

    {% do run_query(sql) %}
{% endmacro %}