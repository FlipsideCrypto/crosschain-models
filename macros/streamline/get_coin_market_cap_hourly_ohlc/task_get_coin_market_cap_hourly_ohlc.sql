{% macro task_bulk_get_coin_market_cap_hourly_ohlc() %}
{% set sql %}
execute immediate 'create or replace task streamline.bulk_get_coin_market_cap_hourly_ohlc
    warehouse = dbt_cloud_crosschain
    allow_overlapping_execution = false
    schedule = \'USING CRON 15,30,45 * * * * UTC\'
as
BEGIN
    call streamline.refresh_external_table_by_recent_date(\'asset_ohlc_coin_market_cap_api\');

    select streamline.udf_bulk_get_coin_market_cap_hourly_ohlc()
    where exists (
        select 1
        from streamline.all_unknown_coin_market_cap_asset_ohlc_hourly
        limit 1
    );
END;'
{% endset %}
{% do run_query(sql) %}

{% if target.database == 'CROSSCHAIN' %}
    {% set sql %}
        alter task streamline.bulk_get_coin_market_cap_hourly_ohlc resume;
    {% endset %}
    {% do run_query(sql) %}
{% endif %}
{% endmacro %}