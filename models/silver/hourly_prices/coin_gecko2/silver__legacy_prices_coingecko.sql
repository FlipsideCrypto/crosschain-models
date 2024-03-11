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
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
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
        _runtime_date,
        _inserted_timestamp
    FROM
        base_legacy
    WHERE
        id IS NOT NULL
)
SELECT
    id,
    recorded_hour,
    OPEN,
    high,
    low,
    CLOSE,
    _runtime_date,
    _inserted_timestamp
FROM
    final_legacy qualify(ROW_NUMBER() over (PARTITION BY id, recorded_hour
ORDER BY
    _inserted_timestamp DESC)) = 1
