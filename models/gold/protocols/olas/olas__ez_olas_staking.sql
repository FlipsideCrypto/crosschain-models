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
    staker_address,
    amount_unadj,
    amount,
    amount_usd,
    token_symbol,
    token_address,
    program_name,
    ez_olas_staking_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ source(
        'gnosis_olas',
        'ez_olas_staking'
    ) }}