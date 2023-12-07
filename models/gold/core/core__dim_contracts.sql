{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'ethereum' AS blockchain
FROM
    {{ ref(
        'silver__contracts'
    ) }}
