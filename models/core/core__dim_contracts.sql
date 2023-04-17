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
            'ethereum_silver',
            'contracts'
        ) }}
    UNION ALL
    SELECT
        contract_address AS address,
        token_symbol AS symbol,
        token_name AS NAME,
        token_decimals AS decimals,
        'optimism' AS blockchain
    FROM
        {{ source(
            'optimism_silver',
            'contracts'
        ) }}
    UNION ALL
    SELECT
        contract_address AS address,
        token_symbol AS symbol,
        token_name AS NAME,
        token_decimals AS decimals,
        'arbitrum' AS blockchain
    FROM
        {{ source(
            'arbitrum_silver',
            'contracts'
        ) }}
    UNION ALL
    SELECT
        contract_address AS address,
        token_symbol AS symbol,
        token_name AS NAME,
        token_decimals AS decimals,
        'polygon' AS blockchain
    FROM
        {{ source(
            'polygon_silver',
            'contracts'
        ) }}
    UNION ALL
    SELECT
        contract_address AS address,
        token_symbol AS symbol,
        token_name AS NAME,
        token_decimals AS decimals,
        'avalanche' AS blockchain
    FROM
        {{ source(
            'avalanche_silver',
            'contracts'
        ) }}
    UNION ALL
    SELECT
        contract_address AS address,
        token_symbol AS symbol,
        token_name AS NAME,
        token_decimals AS decimals,
        'bsc' AS blockchain
    FROM
        {{ source(
            'bsc_silver',
            'contracts'
        ) }}
    UNION ALL
    SELECT
        address AS address,
        LOWER(project_name) AS symbol,
        label AS NAME,
        DECIMAL AS decimals,
        'cosmos' AS blockchain
    FROM
        {{ source(
            'osmosis_silver',
            'asset_metadata'
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
