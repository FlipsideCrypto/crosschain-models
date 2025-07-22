{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BRIDGE',
    } } },
    tags = ['bridge']
) }}

SELECT
    blockchain,
    platform,
    protocol,
    protocol_version,
    block_number,
    block_timestamp,
    tx_hash,
    COALESCE(s1.standardized_name, b.source_chain) AS source_chain,
    COALESCE(s2.standardized_name, b.destination_chain) AS destination_chain,
    bridge_address,
    source_address,
    destination_address,
    direction,
    token_address,
    token_symbol,
    amount_raw,
    amount,
    amount_usd,
    token_is_verified,
    inserted_timestamp,
    modified_timestamp,
    complete_bridge_activity_id AS ez_bridge_activity_id
FROM
    {{ ref('silver__complete_bridge_activity') }} b 

    LEFT JOIN {{ ref('silver_bridge__standard_chain_seed') }} s1 
    ON b.source_chain = s1.variation

    LEFT JOIN {{ ref('silver_bridge__standard_chain_seed') }} s2 
    ON b.destination_chain = s2.variation