{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'platform'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

SELECT
    id,
    i.value :: STRING AS token_address,
    NAME,
    symbol,
    i.key AS platform,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['token_address','platform']) }} AS asset_metadata_coin_gecko_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__asset_metadata_coin_gecko') }} A,
    TABLE(FLATTEN(A.value :platforms)) i

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '24 hours'
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY token_address, platform
ORDER BY
    _inserted_timestamp DESC)) = 1
