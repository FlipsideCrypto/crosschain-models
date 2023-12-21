{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS", False) %}
        {% set sql %}
            CREATE schema if NOT EXISTS streamline;
            {{ udf_bulk_get_coin_gecko_asset_metadata() }};
            {{ () }};
            {{ udf_bulk_get_coin_market_cap_hourly_ohlc() }};
            {{ udf_bulk_get_coin_gecko_ohlc() }};
            {{ udf_bulk_get_coin_gecko_asset_market_data_historical() }};
            {{ create_udf_hex_to_int(
            schema = "public"
        ) }}
        {{ create_udf_hex_to_int_with_inputs(
            schema = "public"
        ) }}
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
