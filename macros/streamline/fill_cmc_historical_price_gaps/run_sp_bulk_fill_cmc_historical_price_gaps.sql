{% macro run_sp_bulk_fill_cmc_historical_price_gaps() %}
    {% set sql %}
        call silver.sp_bulk_fill_cmc_historical_price_gaps();
    {% endset %}

    {% do run_query(sql) %}
{% endmacro %}