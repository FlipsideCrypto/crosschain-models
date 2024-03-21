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