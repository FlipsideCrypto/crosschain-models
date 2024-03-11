-- depends_on: {{ ref('bronze__streamline_hourly_prices_coinmarketcap_sp') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['id','recorded_hour'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE','_inserted_timestamp::DATE']
) }}

WITH legacy AS (

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
        {{ ref('silver__legacy_prices_coinmarketcap') }}
    WHERE
        id IS NOT NULL
        AND recorded_hour IS NOT NULL

{% if is_incremental() %}
AND 1 = 2
{% endif %}
),
base_streamline AS (
    SELECT
        'streamline' AS source,
        _inserted_date AS _runtime_date,
        TRY_TO_NUMBER(
            b.key :: STRING
        ) AS id,
        b.value :quotes [0] :quote :USD :timestamp :: TIMESTAMP AS recorded_timestamp,
        DATE_TRUNC(
            'hour',
            recorded_timestamp
        ) AS recorded_hour,
        b.value :quotes [0] :quote :USD :open :: FLOAT AS OPEN,
        b.value :quotes [0] :quote :USD :high :: FLOAT AS high,
        b.value :quotes [0] :quote :USD :low :: FLOAT AS low,
        b.value :quotes [0] :quote :USD :close :: FLOAT AS CLOSE,
        b.value :quotes [0] :quote :USD :volume :: FLOAT AS volume,
        b.value :quotes [0] :quote :USD :market_cap :: FLOAT AS market_cap,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_hourly_prices_coinmarketcap') }} A,
        LATERAL FLATTEN(
            input => DATA :data
        ) b
    WHERE
        DATA :: STRING <> '[]'
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
final_streamline AS (
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
        base_streamline
    WHERE
        id IS NOT NULL
        AND recorded_hour IS NOT NULL
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
        final_streamline
)
SELECT
    id :: INTEGER AS id,
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
