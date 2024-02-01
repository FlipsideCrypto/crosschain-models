{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', id, recorded_hour)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE','_inserted_timestamp::DATE'],
) }}

WITH base AS (

    SELECT
        _runtime_date,
        id,
        TO_TIMESTAMP(
            f.value [0] :: STRING
        ) AS recorded_timestamp,
        DATE_TRUNC(
            'hour',
            recorded_timestamp
        ) AS recorded_hour,
        f.value [1] AS price,
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
        {{ ref('bronze__hourly_prices_coin_gecko2') }}
        s,
        LATERAL FLATTEN(input => DATA :prices) f

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
FINAL AS (
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
        _runtime_date,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        base
    WHERE
        id IS NOT NULL
    GROUP BY
        recorded_hour,
        id,
        _runtime_date
)
SELECT
    id,
    recorded_hour,
    open,
    high,
    low,
    close,
    _runtime_date,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['id','recorded_hour']) }} AS hourly_prices_coin_gecko_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY id, recorded_hour
ORDER BY
    _inserted_timestamp DESC)) = 1
