{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, SWAPS',
    } } }
) }}

SELECT
    blockchain,
    platform,
    protocol,
    protocol_version,
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    trader,
    token_in,
    symbol_in,
    token_in_is_verified,
    amount_in_raw,
    amount_in,
    amount_in_usd,
    token_out,
    token_out_is_verified,
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
