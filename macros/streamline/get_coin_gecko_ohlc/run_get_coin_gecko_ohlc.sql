{% macro run_get_coin_gecko_ohlc() %}
    {% set sql %}
        call streamline.get_coin_gecko_ohlc();
    {% endset %}

    {% do run_query(sql) %}
{% endmacro %}