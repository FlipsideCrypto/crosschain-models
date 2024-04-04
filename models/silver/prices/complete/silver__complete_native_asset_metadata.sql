{{ config(
    materialized = 'incremental',
    unique_key = ['complete_native_asset_metadata_id'],
    incremental_strategy = 'delete+insert',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(symbol, blockchain)",
    tags = ['prices']
) }}

SELECT
    id AS asset_id,
    UPPER(symbol) AS symbol,
    blockchain,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    native_asset_metadata_priority_id AS complete_native_asset_metadata_id
FROM
    {{ ref('silver__native_asset_metadata_priority') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
