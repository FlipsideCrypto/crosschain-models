{% macro sp_create_copy_coin_gecko_price_data_to_legacy(target_schema) %}
{% set sql %}
create or replace procedure silver.copy_coin_gecko_price_data_to_legacy(legacy_db_name string)
returns boolean 
language javascript
execute as caller
as
$$
    snowflake.execute({sqlText: `BEGIN TRANSACTION;`});
    try {
        stmt = `merge into ${LEGACY_DB_NAME}.silver.prices_v2 a using (
                    with base_metadata as (
                        select *
                        from flipside_prod_db.silver.market_asset_metadata A
                        where (
                          A.platform_id = '1027'
                          OR A.asset_id = '1027'
                          OR A.platform_id = 'ethereum'
                        )
                        AND (
                          token_address IS NOT NULL
                          OR symbol = 'ETH'
                        )
                    )
                    select 
                        distinct a.id as asset_id, 
                        'coingecko' as provider,
                        to_timestamp_ntz(d.value[0]::number,3) as recorded_at, 
                        d.value[4]::float as price,
                        md.name, 
                        md.symbol
                    from streamline.{{ target.database }}.asset_ohlc_coin_gecko_api a
                    left outer join base_metadata md on md.asset_id = a.id
                    left join table(flatten(data)) d
                    where recorded_at >= current_date - 1
                    qualify(row_number() over (partition by asset_id, provider, recorded_at, name, symbol order by price)) = 1
                ) b 
                on a.asset_id = b.asset_id 
                    and a.provider = b.provider 
                    and a.recorded_at = b.recorded_at 
                    and a.name = b.name 
                    and a.symbol = b.symbol
                when matched then 
                    update 
                    set a.price = b.price
                when not matched then 
                    insert (asset_id, provider, recorded_at, name, symbol, price)
                    values (b.asset_id, b.provider, b.recorded_at, b.name, b.symbol, b.price);`
         snowflake.execute({sqlText: stmt}); 
         snowflake.execute({sqlText: `COMMIT;`});
     } catch (err) {
        snowflake.execute({sqlText: `ROLLBACK;`});
        throw(err);
    }
    
    return true
$$;
{% endset %}
{% do run_query(sql) %}

{% endmacro %}