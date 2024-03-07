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
        {{ ref('bronze__streamline_hourly_prices_coinmarketcap_realtime') }}

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
{% else %}
WHERE _inserted_date >= '2022-07-22'
{% endif %}
)
SELECT
    A.id,
    DATE_TRUNC(
        'hour',
        f.value :quote :USD :timestamp :: timestamp_ntz
    ) AS recorded_hour,
    f.value :quote :USD :open::float AS OPEN,
    f.value :quote :USD :high::float AS high,
    f.value :quote :USD :low::float AS low,
    f.value :quote :USD :close::float AS CLOSE,
    f.value :quote :USD :volume::number AS volume,
    f.value :quote :USD :market_cap::number AS market_cap,
    A._inserted_timestamp,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['A.id','recorded_hour']) }} AS hourly_prices_coin_market_cap_id,
    '{{ invocation_id }}' as _invocation_id
FROM
    base A,
    TABLE(FLATTEN(DATA :data :quotes)) f qualify(ROW_NUMBER() over (PARTITION BY id, recorded_hour
ORDER BY
    _inserted_timestamp DESC)) = 1
