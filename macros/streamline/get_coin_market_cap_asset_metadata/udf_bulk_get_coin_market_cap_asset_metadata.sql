{% macro udf_bulk_get_coin_market_cap_asset_metadata() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver.udf_bulk_get_coin_market_cap_asset_metadata() returns text api_integration = aws_crosschain_api_dev AS {% if target.name == "prod" -%}
        'https://q2il6n5mmg.execute-api.us-east-1.amazonaws.com/prod/bulk_get_coin_market_cap_asset_metadata'
    {% else %}
        'https://ubuxgfotp2.execute-api.us-east-1.amazonaws.com/dev/bulk_get_coin_market_cap_asset_metadata'
    {%- endif %}
{% endmacro %}