{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    chain,
    chain_id,
    token_symbol
FROM {{ ref('silver__defillama_chains') }}