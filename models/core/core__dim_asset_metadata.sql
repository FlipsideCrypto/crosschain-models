{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    token_address,
    id,
    symbol,
    decimals,
    blockchain,
    provider
FROM {{ ref('silver__asset_metadata_all_providers') }}
