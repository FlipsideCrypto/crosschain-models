{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(token_address, blockchain)"
) }}

SELECT
    token_address,
    id,
    symbol,
    blockchain,
    provider,
     {{ dbt_utils.surrogate_key( 
        ['token_address','blockchain','provider'] ) }} AS _unique_key,
    _inserted_timestamp
FROM {{ ref('silver__asset_metadata_all_providers')}}
{% if is_incremental() %}
WHERE _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
    )
{% endif %}
QUALIFY(ROW_NUMBER() OVER (PARTITION BY token_address, blockchain, provider
    ORDER BY _inserted_timestamp DESC)) = 1