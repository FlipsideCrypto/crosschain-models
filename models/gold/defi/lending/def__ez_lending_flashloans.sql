{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH base AS (

    SELECT
        'ethereum' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        protocol_token,
        borrow_asset,
        borrow_amount,
        borrow_amount_usd,
        borrow_symbol,
        borrower_address,
        borrow_rate_mode,
        lending_pool_contract
    FROM
        {{ source(
            'ethereum_defi',
            'ez_lending_flashloans'
        ) }}
)
SELECT
    blockchain,
    platform,
    block_number
    block_timestamp
    tx_hash
    contract_address
    pool_address,
    pool_name,
    tokens,
    symbols,
    decimals
FROM
    base
