{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(token_address, blockchain)",
    merge_exclude_columns = ["inserted_timestamp"],
) }}

SELECT
    token_address,
    id,
    symbol,
    blockchain,
    provider,
    {{ dbt_utils.generate_surrogate_key(['token_address','blockchain','provider']) }} AS _unique_key,
    _inserted_timestamp,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['token_address','blockchain','provider']) }} AS asset_metadata_priority_id,
    '{{ invocation_id }}' as _invocation_id
FROM
    {{ ref('silver__asset_metadata_all_providers') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY token_address, blockchain, provider
ORDER BY
    _inserted_timestamp DESC)) = 1
