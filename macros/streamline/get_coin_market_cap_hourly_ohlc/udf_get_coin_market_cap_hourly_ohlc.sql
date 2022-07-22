{% macro udf_bulk_get_coin_market_cap_hourly_ohlc() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_get_coin_market_cap_hourly_ohlc() returns text api_integration = aws_crosschain_api_dev AS {% if target.name == "prod" -%}
        'https://q2il6n5mmg.execute-api.us-east-1.amazonaws.com/prod/bulk_get_coin_market_cap_hourly_ohlc'
    {% else %}
        'https://ubuxgfotp2.execute-api.us-east-1.amazonaws.com/dev/bulk_get_coin_market_cap_hourly_ohlc'
    {%- endif %}
{% endmacro %}