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
    contract_address,
    origin_from_address,
    origin_to_address,
    protocol_market,
    platform,
    depositor,
    token_address,
    token_symbol,
    amount_raw,
    amount, 
    amount_usd,
    complete_lending_withdraws_id AS ez_lending_withdraws_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver__complete_lending_withdraws') }}