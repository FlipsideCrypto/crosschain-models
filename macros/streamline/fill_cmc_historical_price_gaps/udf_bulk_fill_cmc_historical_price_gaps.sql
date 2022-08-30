{% macro udf_bulk_fill_cmc_historical_price_gaps() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver.udf_bulk_fill_cmc_historical_price_gaps() returns text api_integration = aws_crosschain_api_dev AS {% if target.database == "CROSSCHAIN" -%}
        'https://q2il6n5mmg.execute-api.us-east-1.amazonaws.com/prod/bulk_fill_coin_market_cap_historical_price_gaps'
    {% else %}
        'https://ubuxgfotp2.execute-api.us-east-1.amazonaws.com/dev/bulk_fill_coin_market_cap_historical_price_gaps'
    {%- endif %}
{% endmacro %}