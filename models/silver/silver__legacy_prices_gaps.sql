{{ config(
    materialized = 'view',
    post_hook = 'call silver.sp_bulk_fill_cmc_historical_price_gaps()'
) }}

-- gto

WITH cte_date (date_rec) AS (

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
        HOUR BETWEEN CURRENT_DATE - 1
        AND DATEADD(
            'minute',
            -1,
            CURRENT_DATE
        )
),
symbols AS (
    SELECT
        DISTINCT asset_id
    FROM
        {% if target.name == 'prod' %}
            {{ source(
                'legacy_db',
                'prices_v2'
            ) }}
        {% else %}
            {{ source(
                'legacy_dev_db',
                'prices_v2'
            ) }}
        {% endif %}
    WHERE
        recorded_at >= CURRENT_DATE - 14
        AND market_cap > 1000
        AND provider = 'coinmarketcap' qualify(ROW_NUMBER() over (PARTITION BY asset_id
    ORDER BY
        recorded_at DESC)) = 1
),
REFERENCE AS (
    SELECT
        *
    FROM
        cte_date d,
        symbols s
),
recorded AS (
    SELECT
        asset_id,
        DATE_TRUNC(
            'hour',
            recorded_at
        ) recorded_at
    FROM
        {% if target.name == 'prod' %}
            {{ source(
                'legacy_db',
                'prices_v2'
            ) }}
        {% else %}
            {{ source(
                'legacy_dev_db',
                'prices_v2'
            ) }}
        {% endif %} p
    WHERE
        recorded_at >= CURRENT_DATE - 1
        AND recorded_at <= DATEADD(
            'minute',
            -1,
            CURRENT_DATE
        )
        AND provider = 'coinmarketcap'
    GROUP BY
        DATE_TRUNC(
            'hour',
            recorded_at
        ),
        p.asset_id
),
pre_final AS (
    SELECT
        DATE_PART(epoch_second, DATEADD('hour', -1, r.date_rec)) AS start_timestamp,
        DATE_PART(epoch_second, DATEADD('hour', 1, r.date_rec)) AS end_timestamp,
        r.asset_id :: NUMBER AS asset_id
    FROM
        REFERENCE r
        LEFT JOIN recorded p
        ON r.date_rec = p.recorded_at
        AND p.asset_id = r.asset_id
    WHERE
        p.recorded_at IS NULL
)
SELECT
    start_timestamp,
    end_timestamp,
    LISTAGG(
        asset_id,
        ','
    ) AS asset_list
FROM
    pre_final
GROUP BY
    1,
    2
