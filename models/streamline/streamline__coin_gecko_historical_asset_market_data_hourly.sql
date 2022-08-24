{{ config(
    materialized = 'view',
) }}

SELECT
    id,
    90 AS days
FROM
    {{ source(
        'crosschain_external',
        'asset_metadata_api'
    ) }}
WHERE
    provider = 'coingecko'
    AND _inserted_date = (
        SELECT
            MAX(_inserted_date)
        FROM
            {{ source(
                'crosschain_external',
                'asset_metadata_api'
            ) }}
        WHERE
            provider = 'coingecko'
    )
EXCEPT
SELECT
    id,
    90
FROM
    {{ source(
        'crosschain_external',
        'asset_historical_hourly_market_data_coin_gecko_api'
    ) }}
