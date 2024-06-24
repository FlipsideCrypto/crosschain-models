{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS',
    'PURPOSE': 'AI, AGENT, SERVICES',
    } } }
) }}

SELECT
    'ethereum' AS blockchain,
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    event_name,
    donor_address,
    service_id,
    NAME,
    description,
    agent_ids,
    eth_amount_unadj,
    eth_amount,
    eth_amount_usd,
    ez_service_donations_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ source(
        'ethereum_olas',
        'ez_service_donations'
    ) }}
