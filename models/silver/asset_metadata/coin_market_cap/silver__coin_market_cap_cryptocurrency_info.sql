{{ config(
    materialized = 'incremental',
    unique_key = "cmc_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
) }}

SELECT
    key AS cmc_id,
    VALUE :name :: STRING AS NAME,
    VALUE :symbol :: STRING AS symbol,
    VALUE AS metadata,
    _inserted_timestamp,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['cmc_id']) }} as coin_market_cap_cryptocurrency_info_id,
    '{{ invocation_id }}' as _invocation_id
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
