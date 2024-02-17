{{ config(
    materialized = 'view',
) }}

WITH dates AS (

    SELECT
        date_day :: timestamp_ltz AS run_time
    FROM
        {{ ref('core__dim_dates') }}
    WHERE
        date_day BETWEEN CURRENT_DATE - INTERVAL '10 day'
        AND CURRENT_DATE
)
SELECT
    id,
    run_time
FROM
    {{ source(
        'bronze_streamline',
        'asset_metadata_coin_gecko_api'
    ) }}
    JOIN dates
    ON TRUE
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
    DATE_TRUNC(
        'day',
        run_time
    ) :: timestamp_ltz AS run_time
FROM
    {{ source(
        'bronze_streamline',
        'asset_ohlc_coin_gecko_api'
    ) }}
