{{ config(
    materialized = 'incremental',
    unique_key = ['id','recorded_hour'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE','_inserted_timestamp::DATE'],
    tags = ['prices']
) }}

WITH legacy AS (

    SELECT
        id,
        recorded_hour,
        OPEN,
        high,
        low,
        CLOSE,
        source,
        _runtime_date,
        _inserted_timestamp
    FROM
        {{ ref('silver__legacy_prices_coingecko') }}

{% if is_incremental() %}
WHERE 1 = 2
{% endif %}
),
base_backfill AS (
    SELECT
        'backfill' AS source,
        _runtime_date,
        id :: STRING AS id,
        TO_TIMESTAMP(
            f.value [0] :: STRING
        ) AS recorded_timestamp,
        DATE_TRUNC(
            'hour',
            recorded_timestamp
        ) AS recorded_hour,
        f.value [1] :: FLOAT AS price,
        ROW_NUMBER() over(
            PARTITION BY recorded_hour,
            id,
            _runtime_date
            ORDER BY
                recorded_timestamp ASC
        ) AS rn_open,
        ROW_NUMBER() over(
            PARTITION BY recorded_hour,
            id,
            _runtime_date
            ORDER BY
                recorded_timestamp DESC
        ) AS rn_close,
        MAX(price) over(
            PARTITION BY recorded_hour,
            id,
            _runtime_date
        ) AS high_price,
        MIN(price) over(
            PARTITION BY recorded_hour,
            id,
            _runtime_date
        ) AS low_price,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_hourly_prices_coingecko_backfill') }}
        s,
        LATERAL FLATTEN(input => DATA :prices) f

{% if is_incremental() %}
WHERE 1 = 2
{% endif %}
),
final_backfill AS (
    SELECT
        id,
        recorded_hour,
        MAX(
            CASE
                WHEN rn_open = 1 THEN price
            END
        ) AS OPEN,
        MAX(high_price) AS high,
        MAX(low_price) AS low,
        MAX(
            CASE
                WHEN rn_close = 1 THEN price
            END
        ) AS CLOSE,
        source,
        _runtime_date,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        base_backfill
    WHERE
        id IS NOT NULL
    GROUP BY
        recorded_hour,
        id,
        _runtime_date,
        source
),
base_history AS (
    SELECT
        'history' AS source,
        _inserted_date AS _runtime_date,
        id :: STRING AS id,
        TO_TIMESTAMP(
            f.value [0] :: STRING
        ) AS recorded_timestamp,
        DATE_TRUNC(
            'hour',
            recorded_timestamp
        ) AS recorded_hour,
        f.value [1] :: FLOAT AS price,
        ROW_NUMBER() over(
            PARTITION BY recorded_hour,
            id,
            _inserted_date
            ORDER BY
                recorded_timestamp ASC
        ) AS rn_open,
        ROW_NUMBER() over(
            PARTITION BY recorded_hour,
            id,
            _inserted_date
            ORDER BY
                recorded_timestamp DESC
        ) AS rn_close,
        MAX(price) over(
            PARTITION BY recorded_hour,
            id,
            _inserted_date
        ) AS high_price,
        MIN(price) over(
            PARTITION BY recorded_hour,
            id,
            _inserted_date
        ) AS low_price,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_hourly_prices_coingecko_history') }}
        s,
        LATERAL FLATTEN(input => DATA :prices) f

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '24 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
final_history AS (
    SELECT
        id,
        recorded_hour,
        MAX(
            CASE
                WHEN rn_open = 1 THEN price
            END
        ) AS OPEN,
        MAX(high_price) AS high,
        MAX(low_price) AS low,
        MAX(
            CASE
                WHEN rn_close = 1 THEN price
            END
        ) AS CLOSE,
        source,
        _runtime_date,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        base_history
    WHERE
        id IS NOT NULL
    GROUP BY
        source,
        recorded_hour,
        id,
        _runtime_date
),
base_realtime AS (
    SELECT
        'realtime' AS source,
        _inserted_date AS _runtime_date,
        id :: STRING AS id,
        TO_TIMESTAMP(
            f.value [0] :: STRING
        ) AS recorded_timestamp,
        CASE
            WHEN recorded_timestamp = DATE_TRUNC(
                'hour',
                recorded_timestamp
            ) THEN recorded_timestamp
            ELSE NULL
        END AS recorded_hour,
        f.value [1] :: FLOAT AS OPEN,
        f.value [2] :: FLOAT AS high,
        f.value [3] :: FLOAT AS low,
        f.value [4] :: FLOAT AS CLOSE,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_hourly_prices_coingecko_realtime') }}
        s,
        LATERAL FLATTEN(
            input => DATA
        ) f
    WHERE
        recorded_hour IS NOT NULL
        AND DATA :: STRING <> '[]'
        AND DATA IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
),
final_realtime AS (
    SELECT
        id,
        recorded_hour,
        OPEN,
        high,
        low,
        CLOSE,
        source,
        _runtime_date,
        _inserted_timestamp
    FROM
        base_realtime
    WHERE
        id IS NOT NULL
),
all_prices AS (
    SELECT
        *
    FROM
        legacy
    UNION ALL
    SELECT
        *
    FROM
        final_backfill
    UNION ALL
    SELECT
        *
    FROM
        final_history
    UNION ALL
    SELECT
        *
    FROM
        final_realtime
)
SELECT
    id,
    recorded_hour,
    OPEN,
    high,
    low,
    CLOSE,
    source,
    _runtime_date,
    _inserted_timestamp
FROM
    all_prices qualify(ROW_NUMBER() over (PARTITION BY id, recorded_hour
ORDER BY
    _inserted_timestamp DESC)) = 1 -- `backfill` data will eventually be merged into `history` external table
