{{ config(
    materialized = 'incremental',
    unique_key = ['id','recorded_hour'],
    incremental_strategy = 'merge',
    cluster_by = ['recorded_hour::DATE','_inserted_timestamp::DATE'],
    full_refresh = false,
    tags = ['stale']
) }}

WITH base_legacy AS (

    SELECT
        'legacy' AS source,
        recorded_at :: DATE AS _runtime_date,
        asset_id :: STRING AS id,
        recorded_at :: TIMESTAMP AS recorded_timestamp,
        DATE_TRUNC(
            'hour',
            recorded_timestamp
        ) AS recorded_hour,
        NULL AS OPEN,
        NULL AS high,
        NULL AS low,
        price :: FLOAT AS CLOSE,
        NULL AS volume,
        market_cap :: FLOAT market_cap,
        recorded_at AS _inserted_timestamp
    FROM
        {{ source(
            'bronze',
            'legacy_prices'
        ) }}
    WHERE
        provider = 'coingecko'

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
final_legacy AS (
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
        base_legacy
    WHERE
        id IS NOT NULL
),
base_sp AS (
    SELECT
        'sp' AS source,
        _runtime_date,
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
        {{ ref('bronze__streamline_hourly_prices_coingecko_sp') }}
        s,
        LATERAL FLATTEN(
            input => DATA
        ) f
    WHERE
        recorded_hour IS NOT NULL
        AND DATA :: STRING <> '[]'
        AND DATA IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
final_sp AS (
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
        base_sp
    WHERE
        id IS NOT NULL
),
all_prices AS (
    SELECT
        *
    FROM
        final_legacy
    UNION ALL
    SELECT
        *
    FROM
        final_sp
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
    _inserted_timestamp DESC)) = 1
