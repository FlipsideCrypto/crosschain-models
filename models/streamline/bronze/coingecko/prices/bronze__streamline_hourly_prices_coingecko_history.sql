{{ config(
    materialized = 'view',
) }}

SELECT
    *,
    TO_TIMESTAMP_NTZ(
        SUBSTR(SPLIT_PART(metadata$filename, '/', 5), 1, 10) :: NUMBER,
        0
    ) AS _inserted_timestamp
FROM
    {{ source(
        'bronze_streamline',
        'asset_market_chart_coin_gecko_api_v2'
    ) }}
    -- endpoint: market_chart
    -- streamline 2.0 external table
    -- to serve as primary `history` external table, but backfill data needs to be merged in