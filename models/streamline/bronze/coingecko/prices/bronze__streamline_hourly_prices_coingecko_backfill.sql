{{ config(
    materialized = 'view',
) }}

WITH meta AS (

    SELECT
        last_modified AS _inserted_timestamp,
        file_name,
        TO_DATE(TO_TIMESTAMP(SPLIT_PART(SPLIT_PART(
            file_name,
            '/',
            5
        ),'_',1)))::STRING AS _partition_key
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "asset_prices_coin_gecko_api") }}'
            )
        )
)
SELECT
    _runtime_date,
    run_time,
    id,
    currency,
    DATA,
    _inserted_timestamp
FROM
    {{ source(
        'bronze_streamline',
        'asset_prices_coin_gecko_api'
    ) }}
    s
    JOIN meta b
    ON s.metadata$filename = b.file_name
    AND s._runtime_date = b._partition_key 
    -- endpoint: market_chart
    -- streamline 1.0 external table
    -- backfill only: '2018-01-01' - '2024-01-15'
    -- data to be merged into streamline 2.0 external table `asset_market_chart_coin_gecko_api_v2`