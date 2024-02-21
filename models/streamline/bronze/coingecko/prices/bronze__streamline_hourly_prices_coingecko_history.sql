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
-- needs backfill data migrated from streamline.crosschain.asset_prices_coin_gecko_api