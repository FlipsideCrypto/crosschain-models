{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH base AS (

    SELECT
        address,
        symbol,
        NAME,
        decimals,
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
        'solana' AS blockchain
    FROM
        {{ source(
            'solana_core',
            'dim_tokens'
        ) }}
)
SELECT
    address,
    symbol,
    NAME,
    decimals,
    blockchain
FROM
    base
