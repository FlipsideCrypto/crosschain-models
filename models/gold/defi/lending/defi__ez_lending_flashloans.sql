{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'LENDING, FLASHLOANS',
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
    initiator,
    target,
    flashloan_token,
    flashloan_token_symbol,
    flashloan_amount_raw,
    flashloan_amount,
    flashloan_amount_usd,
    premium_amount_raw,
    premium_amount,
    premium_amount_usd,
    complete_lending_flashloans_id AS ez_lending_flashloans_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver__complete_lending_flashloans') }}