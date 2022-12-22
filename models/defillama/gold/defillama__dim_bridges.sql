{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    bridge_id,
    bridge,
    chains,
    destination_chain
FROM {{ ref('silver__defillama_bridges') }}