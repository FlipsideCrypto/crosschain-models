{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH base_priority AS (

SELECT
    token_address,
    id,
    COALESCE(c.symbol,s.symbol) AS symbol,
    name,
    decimals,
    s.blockchain,
    provider,
    CASE
        WHEN provider = 'coingecko' THEN 1
        WHEN provider = 'coinmarketcap' THEN 2
    END AS priority
FROM {{ ref('silver__asset_metadata_priority') }} s
LEFT JOIN {{ ref('core__dim_contracts') }} c 
    ON LOWER(c.address) = s.token_address AND c.blockchain = s.blockchain
)

SELECT
    token_address,
    id,
    symbol,
    name,
    decimals,
    blockchain
FROM base_priority
QUALIFY(ROW_NUMBER() OVER (PARTITION BY token_address, blockchain
    ORDER BY priority ASC)) = 1