{{ config(
    materialized = 'incremental',
    unique_key = "cmc_id",
    incremental_strategy = 'merge'
) }}

SELECT
    key AS cmc_id,
    VALUE :name :: STRING AS NAME,
    VALUE :symbol :: STRING AS symbol,
    VALUE AS metadata,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__coin_market_cap_cryptocurrency_info') }},
    LATERAL FLATTEN(DATA :data :data)

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY key
ORDER BY
    _inserted_timestamp DESC)) = 1
