{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS',
    'PURPOSE': 'AI, AGENT, SERVICES',
    } } }
) }}

SELECT
    'gnosis' AS blockchain,
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    event_name,
    service_id,
    NAME,
    description,
    reward_unadj,
    reward,
    epoch,
    epoch_length,
    total_available_rewards_unadj,
    total_available_rewards,
    program_name,
    ez_service_checkpoints_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ source(
        'gnosis_olas',
        'ez_service_checkpoints'
    ) }}
