{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    chain,
    tvl_usd,
    TIMESTAMP :: DATE AS DATE
FROM
    {{ ref('silver__defillama_chains_tvl') }}
