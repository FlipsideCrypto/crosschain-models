{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'LENDING, LIQUIDATIONS',
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
    liquidator,
    borrower,
    collateral_token,
    collateral_token_symbol,
    amount_raw,
    amount,
    amount_usd,
    debt_token,
    debt_token_symbol,
    complete_lending_liquidations_id AS ez_lending_liquidations_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver__complete_lending_liquidations') }}