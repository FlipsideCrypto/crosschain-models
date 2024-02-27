{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'platform'],
    incremental_strategy = 'delete+insert'
) }}

SELECT
    id,
    i.value :: STRING AS token_address,
    NAME,
    symbol,
    i.key AS platform,
    _inserted_timestamp,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['id','token_address']) }} as asset_metadata_coin_gecko_id,
    '{{ invocation_id }}' as _invocation_id
FROM
    {{ ref('bronze__asset_metadata_coin_gecko') }} A,
    TABLE(FLATTEN(A.value :platforms)) i

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

qualify(ROW_NUMBER() over (PARTITION BY token_address, platform
ORDER BY
    _inserted_timestamp DESC)) = 1
