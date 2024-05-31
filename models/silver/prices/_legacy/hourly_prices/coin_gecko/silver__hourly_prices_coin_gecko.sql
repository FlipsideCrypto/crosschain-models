{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', id, recorded_hour)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE','_inserted_timestamp::DATE'],
) }}

WITH base AS (

    SELECT
        id,
        TO_TIMESTAMP_NTZ(
            d.value [0] :: NUMBER,
            3
        ) AS recorded_hour,
        d.value [1] :: FLOAT AS OPEN,
        d.value [2] :: FLOAT AS high,
        d.value [3] :: FLOAT AS low,
        d.value [4] :: FLOAT AS CLOSE,
        recorded_hour :: DATE AS recorded_date_part,
        HOUR(recorded_hour) AS recorded_hour_part,
        MINUTE(recorded_hour) AS recorded_minute_part,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_hourly_prices_coingecko_realtime') }}
        LEFT JOIN TABLE(FLATTEN(DATA)) d

{% if is_incremental() %}
WHERE
    _inserted_date >= (
        SELECT
            MAX(
                _inserted_timestamp :: DATE
            )
        FROM
            {{ this }}
    )
    AND _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    b.id,
    b.recorded_hour,
    b.open AS OPEN,
    GREATEST(
        b.high,
        b2.high
    ) AS high,
    LEAST(
        b.low,
        b2.low
    ) AS low,
    b2.close AS CLOSE,
    b2._inserted_timestamp,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['b.id','b.recorded_hour']) }} AS hourly_prices_coin_gecko_id,
    '{{ invocation_id }}' as _invocation_id
FROM
    base b
    INNER JOIN base b2
    ON b.id = b2.id
    AND b.recorded_date_part = b2.recorded_date_part
    AND b.recorded_hour_part = b2.recorded_hour_part
    AND b.recorded_minute_part = 0
    AND b2.recorded_minute_part = 30
WHERE
    b2.id IS NOT NULL qualify(ROW_NUMBER() over (PARTITION BY b.id, b.recorded_hour
ORDER BY
    b2._inserted_timestamp DESC)) = 1
