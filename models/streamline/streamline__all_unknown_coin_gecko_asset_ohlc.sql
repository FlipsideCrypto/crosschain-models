{{ config(
    materialized = 'view',
) }}

SELECT
    id,
    date_trunc('hour',current_timestamp) AS run_time
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
    run_time
FROM
    {{ source(
        'crosschain_external',
        'asset_ohlc_coingecko_api'
    ) }}
