{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    token_address,
    id,
    COALESCE(c.symbol,s.symbol) AS symbol,
    name,
    decimals,
    s.blockchain,
    provider
FROM {{ ref('silver__asset_metadata_all_providers') }} s
LEFT JOIN {{ ref('core__dim_contracts') }} c 
    ON LOWER(c.address) = s.token_address AND c.blockchain = s.blockchain
QUALIFY(ROW_NUMBER() OVER (PARTITION BY token_address, id, COALESCE(c.symbol,s.symbol), s.blockchain 
    ORDER BY provider)) = 1