{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    asset_id,
    symbol,
    blockchain,
    is_deprecated,
    inserted_timestamp,
    modified_timestamp,
    complete_native_asset_metadata_id AS ez_native_asset_metadata_id
FROM
    {{ ref('silver__complete_native_asset_metadata') }}
