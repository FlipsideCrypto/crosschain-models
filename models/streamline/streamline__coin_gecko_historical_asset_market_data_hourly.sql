{{ config(
    materialized = 'view',
) }}

SELECT
    id,
    90 AS days
FROM
    {{ source(
        'bronze_streamline',
        'asset_metadata_coin_gecko_api'
    ) }}
WHERE
    provider = 'coingecko'
    AND _inserted_date = (
        SELECT
            MAX(_inserted_date)
        FROM
            {{ source(
                'bronze_streamline',
                'asset_metadata_coin_gecko_api'
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
        'bronze_streamline',
        'asset_historical_hourly_market_data_coin_gecko_api'
    ) }}
