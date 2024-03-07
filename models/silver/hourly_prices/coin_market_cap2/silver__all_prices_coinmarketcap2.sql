{{ config(
    materialized = 'incremental',
    unique_key = ['id','recorded_hour'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE','_inserted_timestamp::DATE']
) }}

WITH base_realtime AS (

    SELECT
        _inserted_date AS _runtime_date,
        id,
        DATE_TRUNC(
            'hour',
            b.value :quote :USD :timestamp :: TIMESTAMP
        ) AS recorded_hour,
        b.value :quote :USD :open :: FLOAT AS OPEN,
        b.value :quote :USD :high :: FLOAT AS high,
        b.value :quote :USD :low :: FLOAT AS low,
        b.value :quote :USD :close :: FLOAT AS CLOSE,
        b.value :quote :USD :volume :: FLOAT AS volume,
        b.value :quote :USD :market_cap :: FLOAT AS market_cap,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_hourly_prices_coinmarketcap_realtime') }} A,
        LATERAL FLATTEN(
            input => DATA :data :quotes
        ) b
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
        volume,
        market_cap,
        _runtime_date,
        _inserted_timestamp
    FROM
        base_realtime
    WHERE
        id IS NOT NULL
),
all_prices AS (
    --add history
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
    volume,
    market_cap,
    _runtime_date,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['id','recorded_hour']) }} AS hourly_prices_coin_gecko_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_prices qualify(ROW_NUMBER() over (PARTITION BY id, recorded_hour
ORDER BY
    _inserted_timestamp DESC)) = 1
