{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, LIQUIDITY, POOLS, LP, SWAPS',
    } } },
    tags = ['dex']
) }}

WITH base AS (

    SELECT
        'ethereum' AS blockchain,
        platform,
        protocol,
        version as protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','pool_address','pool_id']) }} AS dim_dex_liquidity_pools_id
    FROM
        {{ source(
            'ethereum_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
    UNION ALL
    SELECT
        'optimism' AS blockchain,
        platform,
        protocol,
        version as protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','pool_address']) }} AS dim_dex_liquidity_pools_id
    FROM
        {{ source(
            'optimism_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
    UNION ALL
    SELECT
        'avalanche' AS blockchain,
        platform,
        protocol,
        version as protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','pool_address']) }} AS dim_dex_liquidity_pools_id
    FROM
        {{ source(
            'avalanche_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
    UNION ALL
    SELECT
        'polygon' AS blockchain,
        platform,
        protocol,
        version as protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','pool_address']) }} AS dim_dex_liquidity_pools_id
    FROM
        {{ source(
            'polygon_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
    UNION ALL
    SELECT
        'bsc' AS blockchain,
        platform,
        protocol,
        version as protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','pool_address']) }} AS dim_dex_liquidity_pools_id
    FROM
        {{ source(
            'bsc_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
    UNION ALL
    SELECT
        'arbitrum' AS blockchain,
        platform,
        protocol,
        version as protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','pool_address']) }} AS dim_dex_liquidity_pools_id
    FROM
        {{ source(
            'arbitrum_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
    UNION ALL
    SELECT
        'base' AS blockchain,
        platform,
        protocol,
        version as protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','pool_address']) }} AS dim_dex_liquidity_pools_id
    FROM
        {{ source(
            'base_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
    UNION ALL
    SELECT
        'gnosis' AS blockchain,
        platform,
        protocol,
        version as protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','pool_address']) }} AS dim_dex_liquidity_pools_id
    FROM
        {{ source(
            'gnosis_silver_dex',
            'complete_dex_liquidity_pools'
        ) }}
)
SELECT
    blockchain,
    platform,
    protocol,
    protocol_version,
    block_number AS creation_block,
    block_timestamp AS creation_time,
    tx_hash AS creation_tx,
    contract_address AS factory_address,
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
