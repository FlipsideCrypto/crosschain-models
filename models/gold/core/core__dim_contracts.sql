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
        created_block_number,
        created_block_timestamp,
        created_tx_hash,
        creator_address,
        'ethereum' AS blockchain,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS dim_contracts_id
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
        'optimism' AS blockchain,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS dim_contracts_id
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
        'arbitrum' AS blockchain,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS dim_contracts_id
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
        'polygon' AS blockchain,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS dim_contracts_id
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
        'avalanche' AS blockchain,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS dim_contracts_id
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
        'bsc' AS blockchain,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS dim_contracts_id
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
        'base' AS blockchain,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS dim_contracts_id
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
        'gnosis' AS blockchain,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS dim_contracts_id
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
        'cosmos' AS blockchain,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS dim_contracts_id
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
        'solana' AS blockchain,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS dim_contracts_id
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
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    blockchain,
    inserted_timestamp,
    modified_timestamp,
    dim_contracts_id
FROM
    {{ ref(
        'silver__contracts'
    ) }}
