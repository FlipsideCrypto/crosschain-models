{{ config(
    materialized = 'view'
) }}

WITH base AS (

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
        {{ source(
            'ethereum_core',
            'dim_contracts'
        ) }}
    UNION ALL
    SELECT
        address,
        symbol,
        NAME,
        decimals,
        created_block_number,
        created_block_timestamp,
        created_tx_hash,
        creator_address,
        'optimism' AS blockchain
    FROM
        {{ source(
            'optimism_core',
            'dim_contracts'
        ) }}
    UNION ALL
    SELECT
        address,
        symbol,
        NAME,
        decimals,
        created_block_number,
        created_block_timestamp,
        created_tx_hash,
        creator_address,
        'arbitrum' AS blockchain
    FROM
        {{ source(
            'arbitrum_core',
            'dim_contracts'
        ) }}
    UNION ALL
    SELECT
        address,
        symbol,
        NAME,
        decimals,
        created_block_number,
        created_block_timestamp,
        created_tx_hash,
        creator_address,
        'polygon' AS blockchain
    FROM
        {{ source(
            'polygon_core',
            'dim_contracts'
        ) }}
    UNION ALL
    SELECT
        address,
        symbol,
        NAME,
        decimals,
        created_block_number,
        created_block_timestamp,
        created_tx_hash,
        creator_address,
        'avalanche' AS blockchain
    FROM
        {{ source(
            'avalanche_core',
            'dim_contracts'
        ) }}
    UNION ALL
    SELECT
        address,
        symbol,
        NAME,
        decimals,
        created_block_number,
        created_block_timestamp,
        created_tx_hash,
        creator_address,
        'bsc' AS blockchain
    FROM
        {{ source(
            'bsc_core',
            'dim_contracts'
        ) }}
    UNION ALL
    SELECT
        address,
        symbol,
        NAME,
        decimals,
        created_block_number,
        created_block_timestamp,
        created_tx_hash,
        creator_address,
        'base' AS blockchain
    FROM
        {{ source(
            'base_core',
            'dim_contracts'
        ) }}
    UNION ALL
    SELECT
        address,
        symbol,
        NAME,
        decimals,
        NULL AS created_block_number,
        NULL AS created_block_timestamp,
        NULL AS created_tx_hash,
        NULL AS creator_address,
        'gnosis' AS blockchain
    FROM
        {{ source(
            'gnosis_core',
            'dim_contracts'
        ) }}
    UNION ALL
    SELECT
        address,
        LOWER(project_name) AS symbol,
        label AS NAME,
        DECIMAL AS decimals,
        NULL AS created_block_number,
        NULL AS created_block_timestamp,
        NULL AS created_tx_hash,
        NULL AS creator_address,
        'cosmos' AS blockchain
    FROM
        {{ source(
            'osmosis_core',
            'dim_tokens'
        ) }}
    UNION ALL
    SELECT
        token_address AS address,
        LOWER(symbol) AS symbol,
        token_name AS NAME,
        decimals,
        NULL AS created_block_number,
        NULL AS created_block_timestamp,
        NULL AS created_tx_hash,
        NULL AS creator_address,
        'solana' AS blockchain
    FROM
        {{ source(
            'solana_core',
            'dim_tokens'
        ) }}
    UNION ALL
    SELECT
        token_address AS address,
        LOWER(symbol) AS symbol,
        NAME AS NAME,
        decimals,
        NULL AS created_block_number,
        transaction_created_timestamp AS created_block_timestamp,
        NULL AS created_tx_hash,
        creator_address AS creator_address,
        'aptos' AS blockchain
    FROM
        {{ source(
            'aptos_core',
            'dim_tokens'
        ) }}
)
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    blockchain
FROM
    base
