{% macro run_sp_copy_coin_gecko_price_data_to_legacy() %}
{% if target.database == 'CROSSCHAIN' %}
    {% set sql %}
    call silver.copy_coin_gecko_price_data_to_legacy('flipside_prod_db');
    {% endset %}
{% else %}
    {% set sql %}
    call silver.copy_coin_gecko_price_data_to_legacy('flipside_dev_db');
    {% endset %}
{% endif %}

{% do run_query(sql) %}
{% endmacro %}