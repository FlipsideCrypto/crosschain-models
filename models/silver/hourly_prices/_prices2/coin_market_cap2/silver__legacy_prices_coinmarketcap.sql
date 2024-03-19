-- depends_on: {{ ref('bronze__streamline_hourly_prices_coinmarketcap_sp') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['id','recorded_hour'],
    incremental_strategy = 'merge',
    cluster_by = ['recorded_hour::DATE','_inserted_timestamp::DATE'],
    tags = ['stale']
) }}

--    full_refresh = false,

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
        provider = 'coinmarketcap'
        AND recorded_at < '2022-07-20'

{% if is_incremental() %}
AND 1 = 2
{% endif %}
),
final_legacy AS (
    SELECT
        id,
        recorded_timestamp,
        recorded_hour,
        OPEN,
        high,
        low,
        CLOSE,
        volume,
        market_cap,
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
        _inserted_date AS _runtime_date,
        A.id :: STRING AS id,
        b.value :quote :USD :timestamp :: TIMESTAMP AS recorded_timestamp,
        DATE_TRUNC(
            'hour',
            recorded_timestamp
        ) AS recorded_hour,
        b.value :quote :USD :open :: FLOAT AS OPEN,
        b.value :quote :USD :high :: FLOAT AS high,
        b.value :quote :USD :low :: FLOAT AS low,
        b.value :quote :USD :close :: FLOAT AS CLOSE,
        b.value :quote :USD :volume :: FLOAT AS volume,
        b.value :quote :USD :market_cap :: FLOAT AS market_cap,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_hourly_prices_coinmarketcap_sp') }} A,
        LATERAL FLATTEN(
            input => DATA :quotes
        ) b
    WHERE
        _inserted_date >= '2022-07-20'
        AND DATA :: STRING <> '[]'
        AND DATA IS NOT NULL

{% if is_incremental() %}
AND 1 = 2
{% endif %}
),
final_sp AS (
    SELECT
        id,
        recorded_timestamp,
        recorded_hour,
        OPEN,
        high,
        low,
        CLOSE,
        volume,
        market_cap,
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
    recorded_timestamp,
    recorded_hour,
    OPEN,
    high,
    low,
    CLOSE,
    volume,
    market_cap,
    source,
    _runtime_date,
    _inserted_timestamp
FROM
    all_prices qualify(ROW_NUMBER() over (PARTITION BY id, recorded_hour
ORDER BY
    _inserted_timestamp DESC)) = 1
