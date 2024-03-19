{{ config(
    materialized = 'view',
) }}

WITH meta AS (

    SELECT
        last_modified AS _inserted_timestamp,
        file_name,
        SPLIT_PART(
            file_name,
            '/',
            4
        ) AS _partition_key
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "asset_market_chart_coin_gecko_api_v2") }}'
            )
        )
)
SELECT
    partition_key,
    DATE_PART('EPOCH',run_time)::STRING AS run_time_epoch,
    run_time,
    _inserted_date,
    id,
    currency,
    DATA,
    _inserted_timestamp
FROM
    {{ source(
        'bronze_streamline',
        'asset_market_chart_coin_gecko_api_v2'
    ) }}
    s
    JOIN meta b
    ON s.metadata$filename = b.file_name
    AND run_time_epoch = b._partition_key 
    -- endpoint: market_chart
    -- streamline 2.0 external table
    -- to serve as primary `history` external table, but backfill data needs to be merged in
