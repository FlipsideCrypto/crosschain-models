{% macro create_udfs() %}
    {% set sql %}
        CREATE schema if NOT EXISTS streamline;
        {{ udf_bulk_fill_cmc_historical_price_gaps() }};
        {{ udf_bulk_get_coin_gecko_asset_metadata() }};
        {{ udf_bulk_get_coin_market_cap_asset_metadata() }};
        {{ udf_bulk_get_coin_market_cap_prices() }};
        {{ udf_bulk_get_coin_gecko_prices() }};
        {{ udf_bulk_get_coin_market_cap_hourly_ohlc() }};
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
