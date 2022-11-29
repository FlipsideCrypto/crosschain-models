{% macro task_run_sp_bulk_get_coin_market_cap_prices(resume_or_suspend) -%}
    create or replace task silver.run_sp_bulk_get_coin_market_cap_prices
        warehouse = dbt_cloud_legacy_prices
        schedule = 'USING CRON * * * * * UTC'
    as
        call silver.sp_bulk_get_coin_market_cap_prices();

    alter task silver.run_sp_bulk_get_coin_market_cap_prices {{ resume_or_suspend }}
{%- endmacro %}