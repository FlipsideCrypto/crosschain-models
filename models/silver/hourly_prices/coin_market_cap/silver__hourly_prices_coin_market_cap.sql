{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', id, recorded_hour)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE','_inserted_timestamp::DATE'],
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('bronze__streamline_hourly_prices_coinmarketcap') }}

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
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    _inserted_date >= '2022-07-22'
{% endif %}
),
FINAL AS (
    SELECT
        TRY_TO_NUMBER(
            f.key :: STRING
        ) AS id,
        DATE_TRUNC(
            'hour',
            f.value :quotes [0] :quote :USD :timestamp :: timestamp_ntz
        ) AS recorded_hour,
        f.value :quotes [0] :quote :USD :open :: FLOAT AS OPEN,
        f.value :quotes [0] :quote :USD :high :: FLOAT AS high,
        f.value :quotes [0] :quote :USD :low :: FLOAT AS low,
        f.value :quotes [0] :quote :USD :close :: FLOAT AS CLOSE,
        f.value :quotes [0] :quote :USD :volume :: FLOAT AS volume,
        f.value :quotes [0] :quote :USD :market_cap :: FLOAT AS market_cap,
        A._inserted_timestamp,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['id','recorded_hour']) }} AS hourly_prices_coin_market_cap_id,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        base A,
        TABLE(FLATTEN(DATA :data)) f
)
SELECT
    *
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY id, recorded_hour
ORDER BY
    _inserted_timestamp DESC)) = 1
