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
        'asset_prices_coin_gecko_api'
    ) }}
    -- endpoint: market_chart
    -- streamline 1.0 external table
    -- backfill only: '2018-01-01' - '2024-01-15'
    -- data to be merged into streamline 2.0 external table `asset_market_chart_coin_gecko_api_v2`