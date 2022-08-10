{{ config(
    materialized = 'view',
) }}

WITH base AS (

    SELECT
        id,
        DATE_PART(
            'epoch',
            GREATEST(
                VALUE :first_historical_data :: timestamp_ntz,
                CURRENT_DATE - 365
            )
        ) AS historical_load_start_time,
        DATE_PART(
            'epoch',
            LEAST(
                VALUE :last_historical_data :: timestamp_ntz,
                '2022-07-19 23:59:59.999' :: timestamp_ntz
            )
        ) AS historical_load_end_time
    FROM
        {{ source(
            'crosschain_external',
            'asset_metadata_api'
        ) }}
    WHERE
        provider = 'coinmarketcap'
        AND _inserted_date = (
            SELECT
                MAX(_inserted_date)
            FROM
                {{ source(
                    'crosschain_external',
                    'asset_metadata_api'
                ) }}
            WHERE
                provider = 'coinmarketcap'
        )
        AND VALUE :last_historical_data :: timestamp_ntz >= CURRENT_DATE - 365
)
SELECT
    historical_load_start_time as start_time,
    historical_load_end_time as end_time,
    id AS asset_ids
FROM
    base
WHERE 
    end_time > start_time
EXCEPT
SELECT
    api_start_time,
    api_end_time,
    id
FROM
    {{ source(
        'crosschain_external',
        'asset_ohlc_coin_market_cap_api'
    ) }}
WHERE
    NULLIF(
        DATA,{}
    ) IS NOT NULL
