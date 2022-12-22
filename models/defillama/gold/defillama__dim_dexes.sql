{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    dex,
    dex_slug,
    category,
    chains 
FROM {{ ref('silver__defillama_dexes') }}