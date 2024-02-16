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
    trader,
    token_in,
    symbol_in,
    amount_in_raw,
    amount_in,
    amount_in_usd,
    token_out,
    symbol_out,
    amount_out_raw,
    amount_out,
    amount_out_usd,
    _log_id,
    inserted_timestamp,
    modified_timestamp,
    complete_dex_swaps_id AS ez_dex_swaps_id
FROM
    {{ ref('silver__complete_dex_swaps') }}
