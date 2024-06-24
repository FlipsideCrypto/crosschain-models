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
    token_address,
    olas_amount_unadj,
    olas_amount,
    olas_amount_usd,
    lp_token_address,
    lp_token_amount_unadj,
    lp_token_amount,
    product_id,
    bond_id,
    maturity_timestamp,
    is_redeemed,
    ez_olas_bonding_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ source(
        'ethereum_olas',
        'ez_olas_bonding'
    ) }}

