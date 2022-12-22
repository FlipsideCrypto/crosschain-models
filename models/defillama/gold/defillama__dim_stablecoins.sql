{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    stablecoin_id,
    stablecoin,
    symbol,
    peg_type,
    peg_mechanism,
    price_source,
    chains
FROM {{ ref('silver__defillama_stablecoins') }}