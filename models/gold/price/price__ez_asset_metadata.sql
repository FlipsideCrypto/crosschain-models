{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH base_priority AS (

    SELECT
        token_address,
        id,
        COALESCE(
            C.symbol,
            s.symbol
        ) AS symbol,
        NAME,
        decimals,
        s.blockchain,
        provider,
        CASE
            WHEN provider = 'coingecko' THEN 1
            WHEN provider = 'coinmarketcap' THEN 2
        END AS priority
    FROM
        {{ ref('silver__asset_metadata_priority') }}
        s
        LEFT JOIN {{ ref('core__dim_contracts') }} C
        ON LOWER(
            C.address
        ) = LOWER(
            s.token_address
        )
        AND C.blockchain = s.blockchain
)
SELECT
    token_address,
    id,
    symbol,
    NAME,
    decimals,
    blockchain
FROM
    base_priority qualify(ROW_NUMBER() over (PARTITION BY token_address, blockchain
ORDER BY
    priority ASC)) = 1
