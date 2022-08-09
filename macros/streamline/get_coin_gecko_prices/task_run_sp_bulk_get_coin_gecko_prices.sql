{% macro task_run_sp_bulk_get_coin_gecko_prices(resume_or_suspend) -%}
    create or replace task silver.run_sp_bulk_get_coin_gecko_prices
        warehouse = dbt_cloud
        schedule = 'USING CRON 15,45 * * * * UTC'
    as
        call silver.sp_bulk_get_coin_gecko_prices();

    alter task silver.run_sp_bulk_get_coin_gecko_prices {{ resume_or_suspend }}
{%- endmacro %}