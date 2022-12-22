{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    protocol_id,
    protocol_slug,
    protocol,
    address,
    symbol,
    description,
    chain,
    chains,
    category,
    num_audits,
    audit_note
FROM {{ ref('silver__defillama_protocols') }}