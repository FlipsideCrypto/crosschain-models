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
    contract_name,
    event_index,
    event_name,
    account_address,
    olas_amount_unadj,
    olas_amount,
    olas_amount_usd,
    unlock_timestamp,
    ez_olas_locking_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ source(
        'ethereum_olas',
        'ez_olas_locking'
    ) }}
