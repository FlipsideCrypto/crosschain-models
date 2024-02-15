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
    amount_in_raw,
    token_out,
    amount_out_raw,
    _log_id,
    inserted_timestamp,
    modified_timestamp,
    complete_dex_swaps_id AS fact_dex_swaps_id
FROM
    {{ ref('silver__complete_dex_swaps') }}
