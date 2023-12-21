{% macro udf_bulk_get_coin_gecko_prices() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_get_coin_gecko_prices(json variant) returns variant api_integration = aws_crosschain_api_dev AS {% if target.database == "CROSSCHAIN" -%}
        'https://35hm1qhag9.execute-api.us-east-1.amazonaws.com/prod/bulk_get_coin_gecko_market_chart_daily'
    {% else %}
        'https://tlbh2d47i2.execute-api.us-east-1.amazonaws.com/dev/bulk_get_coin_gecko_market_chart_daily'
    {%- endif %}
{% endmacro %}