{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'LENDING, WITHDRAWS',
    } } }
) }}

SELECT
    blockchain,
    platform,
    protocol,
    version,
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
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