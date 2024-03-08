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
            information_schema.external_table_file_registration_history(
                table_name => '{{ source( "bronze_streamline", "asset_ohlc_coin_market_cap_api") }}'
            )
        )
)
SELECT
    api_end_time AS partition_key,
    _inserted_date,
    TO_TIMESTAMP(partition_key) AS run_time,
    id,
    DATA,
    _inserted_timestamp
FROM
    {{ source(
        'bronze_streamline',
        'asset_ohlc_coin_market_cap_api'
    ) }}
    s
    JOIN meta b
    ON s.metadata$filename = b.file_name
    -- endpoint: ohlc
    -- streamline 1.0 external table
    -- destination for stored procedures pipeline, containing historical data