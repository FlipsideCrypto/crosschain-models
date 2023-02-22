{% macro sp_get_coin_gecko_ohlc() %}
{% set sql %}
create or replace procedure streamline.get_coin_gecko_ohlc()
returns string
language sql
execute as caller
as
$$
    DECLARE
        select_stmt string;
        res RESULTSET;
    BEGIN
        select_stmt := 'call streamline.refresh_external_table_by_recent_date(\'asset_metadata_coin_gecko_api\');';
        res := (EXECUTE IMMEDIATE :select_stmt);

        select_stmt := 'call streamline.refresh_external_table_by_recent_date(\'asset_ohlc_coin_gecko_api\');';
        res := (EXECUTE IMMEDIATE :select_stmt);

        select_stmt := 'select streamline.udf_bulk_get_coin_gecko_ohlc()
        where exists (
            select 1
            from streamline.all_unknown_coin_gecko_asset_ohlc
            limit 1
        );';
        res := (execute immediate :select_stmt);

        RETURN 'SUCCESS';
    END;
$${% endset %}
{% do run_query(sql) %}
{% endmacro %}