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
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE(complete_dex_liquidity_pools_id,{{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform','version','tx_hash']) }}) AS dim_dex_liquidity_pools_id
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
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE(complete_dex_liquidity_pools_id,{{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform','version','tx_hash']) }}) AS dim_dex_liquidity_pools_id
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
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE(complete_dex_liquidity_pools_id,{{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform','version','tx_hash']) }}) AS dim_dex_liquidity_pools_id
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
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE(complete_dex_liquidity_pools_id,{{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform','version','tx_hash']) }}) AS dim_dex_liquidity_pools_id
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
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE(complete_dex_liquidity_pools_id,{{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform','version','tx_hash']) }}) AS dim_dex_liquidity_pools_id
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
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE(complete_dex_liquidity_pools_id,{{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform','version','tx_hash']) }}) AS dim_dex_liquidity_pools_id
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
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE(complete_dex_liquidity_pools_id,{{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform','version','tx_hash']) }}) AS dim_dex_liquidity_pools_id
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
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE(complete_dex_liquidity_pools_id,{{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform','version','tx_hash']) }}) AS dim_dex_liquidity_pools_id
    FROM
        {{ source(
            'gnosis_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
)
SELECT
    blockchain,
    platform,
    block_number as creation_block,
    block_timestamp as creation_time,
    tx_hash as creation_tx,
    contract_address as factory_address,
    pool_address,
    pool_name,
    tokens,
    symbols,
    decimals,
    inserted_timestamp,
    modified_timestamp,
    dim_dex_liquidity_pools_id
FROM
    base
