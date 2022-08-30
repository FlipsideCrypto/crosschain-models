{% macro task_bulk_get_coin_gecko_asset_market_data_historical() %}
{% set sql %}
execute immediate 'create or replace task streamline.bulk_get_coin_gecko_asset_market_data_historical
    warehouse = dbt_cloud_crosschain
    allow_overlapping_execution = false
    schedule = \'USING CRON */15 * * * * UTC\'
as
BEGIN
    call streamline.refresh_external_table_by_recent_date(\'asset_historical_hourly_market_data_coin_gecko_api\');

    select streamline.udf_bulk_get_coin_gecko_asset_market_data_historical()
    where exists (
        select 1
        from streamline.coin_gecko_historical_asset_market_data_hourly
        limit 1
    );
END;'
{% endset %}
{% do run_query(sql) %}

{% if target.database == 'CROSSCHAIN' %}
    {% set sql %}
        alter task streamline.bulk_get_coin_gecko_asset_market_data_historical resume;
    {% endset %}
    {% do run_query(sql) %}
{% endif %}
{% endmacro %}