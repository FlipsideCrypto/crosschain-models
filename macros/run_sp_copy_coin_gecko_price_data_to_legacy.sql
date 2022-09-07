{% macro run_sp_copy_coin_gecko_price_data_to_legacy() %}
{% set sql %}
call crosschain.silver.copy_coin_gecko_price_data_to_legacy('flipside_prod_db');
{% endset %}

{% do run_query(sql) %}
{% endmacro %}