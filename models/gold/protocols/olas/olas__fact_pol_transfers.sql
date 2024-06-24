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
    from_address,
    to_address,
    lp_token_address,
    lp_token_name,
    lp_token_amount_unadj,
    lp_token_amount,
    source_chain,
    fact_pol_transfers_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ source(
        'ethereum_olas',
        'fact_pol_transfers'
    ) }}
