{{ config(
    materialized = 'view',
) }}

WITH hours AS (

    SELECT
        HOUR
    FROM
        {% if target.name == 'prod' %}
            {{ source(
                'legacy_db',
                'hours'
            ) }}
        {% else %}
            {{ source(
                'legacy_dev_db',
                'hours'
            ) }}
        {% endif %}
    WHERE
        HOUR >= '2022-07-20'
        AND HOUR < DATE_TRUNC(
            'hour',
            CURRENT_TIMESTAMP
        ) -- the hour should always be less than current time because it must be "completed" before ohlcv is available
),
cmc_active_assets AS (
    SELECT
        id::number as id
    FROM
        {{ source(
            'crosschain_external',
            'asset_metadata_api'
        ) }}
    WHERE
        provider = 'coinmarketcap'
        AND VALUE :status :: STRING = 'active'
),
base AS (
    SELECT
        DATE_PART('epoch', DATEADD('minute', -1, HOUR)) AS start_time,
        DATE_PART('epoch', DATEADD('hour', 1, HOUR)) AS end_time,
        id
    FROM
        cmc_active_assets
        CROSS JOIN hours
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
),
base_params AS (
    SELECT
        start_time,
        end_time,
        id,
        1 AS cnt,
        SUM(cnt) over (
            PARTITION BY start_time,
            end_time
            ORDER BY
                id
        ) AS csum,
        CEIL(
            csum / 1000
        ) AS group_cnt
    FROM
        base
)
SELECT
    start_time,
    end_time,
    group_cnt,
    LISTAGG(
        id,
        ','
    ) AS asset_ids
FROM
    base_params
GROUP BY
    1,
    2,
    3
