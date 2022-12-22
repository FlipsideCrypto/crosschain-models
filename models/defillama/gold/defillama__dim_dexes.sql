{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    dex_slug,
    dex,
    category,
    chains 
FROM {{ ref('silver__defillama_dexes') }}