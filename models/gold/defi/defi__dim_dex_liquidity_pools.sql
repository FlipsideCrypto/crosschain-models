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
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        _id
    FROM
        {{ source(
            'ethereum_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
    UNION ALL
    SELECT
        'optimism' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        _id
    FROM
        {{ source(
            'optimism_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
    UNION ALL
    SELECT
        'avalanche' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        _id
    FROM
        {{ source(
            'avalanche_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
    UNION ALL
    SELECT
        'polygon' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        _id
    FROM
        {{ source(
            'polygon_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
    UNION ALL
    SELECT
        'bsc' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        _id
    FROM
        {{ source(
            'bsc_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
    UNION ALL
    SELECT
        'arbitrum' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        _id
    FROM
        {{ source(
            'arbitrum_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
    UNION ALL
    SELECT
        'base' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        _id
    FROM
        {{ source(
            'base_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
    UNION ALL
    SELECT
        'gnosis' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        _id
    FROM
        {{ source(
            'gnosis_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
)
SELECT
    blockchain,
    block_number as creation_block,
    block_timestamp as creation_time,
    tx_hash as creation_tx,
    platform,
    contract_address as factory_address,
    pool_address,
    pool_name,
    tokens,
    symbols,
    decimals,
    _id
FROM
    base
