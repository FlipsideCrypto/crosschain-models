{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    token_address,
    id,
    symbol,
    NAME,
    platform AS blockchain,
    platform_id AS blockchain_id,
    provider,
    inserted_timestamp,
    modified_timestamp,
    all_asset_metadata_all_providers_id AS dim_asset_metadata_id
FROM
    {{ ref('silver__all_asset_metadata_all_providers2') }} A
