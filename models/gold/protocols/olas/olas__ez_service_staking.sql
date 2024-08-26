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
    s.block_number,
    s.block_timestamp,
    s.tx_hash,
    s.origin_function_signature,
    s.origin_from_address,
    s.origin_to_address,
    s.contract_address,
    s.event_index,
    s.event_name,
    s.owner_address,
    s.multisig_address,
    s.service_id,
    m.name,
    m.description,
    s.epoch,
    s.reward_unadj,
    s.reward_adj AS reward,
    s.available_rewards_unadj AS total_available_rewards_unadj,
    s.available_rewards_adj AS total_available_rewards,
    s.nonces,
    s.program_name,
    s.service_staking_id AS ez_service_staking_id,
    s.inserted_timestamp,
    GREATEST(
        COALESCE(
            s.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        ),
        COALESCE(
            m.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        )
    ) AS modified_timestamp
FROM
    {{ source(
        'gnosis_silver_olas',
        'service_staking'
    ) }}
    s
    LEFT JOIN {{ source(
        'gnosis_silver_olas',
        'registry_metadata_complete'
    ) }}
    m
    ON s.service_id = m.registry_id
