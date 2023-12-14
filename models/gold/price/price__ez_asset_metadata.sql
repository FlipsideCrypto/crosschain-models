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
        END AS priority,
        GREATEST(COALESCE(s.inserted_timestamp,'2000-01-01'), COALESCE(C.inserted_timestamp,'2000-01-01')) as inserted_timestamp,
        GREATEST(COALESCE(s.modified_timestamp,'2000-01-01'), COALESCE(C.modified_timestamp,'2000-01-01')) as modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['token_address','s.blockchain']) }} AS ez_asset_metadata_id
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
    blockchain,
    inserted_timestamp,
    modified_timestamp,
    ez_asset_metadata_id
FROM
    base_priority qualify(ROW_NUMBER() over (PARTITION BY token_address, blockchain
ORDER BY
    priority ASC)) = 1
