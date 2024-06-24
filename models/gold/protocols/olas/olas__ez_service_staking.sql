{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS',
    'PURPOSE': 'AI, AGENT, SERVICES',
    } } }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    event_name,
    owner_address,
    multisig_address,
    service_id,
    NAME,
    description,
    epoch,
    nonces,
    program_name,
    ez_service_staking_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ source(
        'gnosis_olas',
        'ez_service_staking'
    ) }}
