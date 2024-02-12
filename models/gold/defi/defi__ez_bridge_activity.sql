{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    blockchain,
    platform,
    block_number,
    block_timestamp,
    tx_hash,
    source_chain,
    destination_chain,
    bridge_address,
    source_address,
    destination_address,
    direction,
    token_address,
    token_symbol,
    amount_raw,
    amount,
    amount_usd,
    inserted_timestamp,
    modified_timestamp,
    complete_bridge_activity_id AS ez_bridge_activity_id
FROM
    {{ ref('silver__complete_bridge_activity') }}
