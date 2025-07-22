{{ config(
    materialized = 'view',
    tags = ['daily']
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
        created_block_number,
        created_block_timestamp,
        created_tx_hash,
        creator_address,
        'blast' AS blockchain,
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
            'blast_core',
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
        'kaia' AS blockchain,
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
            'kaia_core',
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
        symbol,
        NAME,
        decimals,
        created_block_number,
        created_block_timestamp,
        created_tx_hash,
        creator_address,
        'mantle' AS blockchain,
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
            'mantle_core',
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
        'core' AS blockchain,
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
            'core_core',
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
        'boba' AS blockchain,
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
            'boba_core',
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
        'sei' AS blockchain,
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
            'sei_evm_core',
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
        'flow' AS blockchain,
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
            'flow_evm_core',
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
        'berachain-bartio' AS blockchain,
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
            'berachain_bartio_testnet',
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
        'ronin' AS blockchain,
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
            'ronin_core',
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
        'swell' AS blockchain,
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
            'swell_core',
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
        'ink' AS blockchain,
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
            'ink_core',
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
        'bob' AS blockchain,
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
            'bob_core',
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
        NAME,
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
            'solana_silver',
            'solscan_token'
        ) }}
        where decimals is not null
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
        'aptos' AS blockchain,
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
            'aptos_core',
            'dim_tokens'
        ) }}
    UNION ALL
    SELECT
        asset_identifier AS address,
        symbol,
        NAME,
        decimals,
        NULL AS created_block_number,
        NULL AS created_block_timestamp,
        NULL AS created_tx_hash,
        NULL AS creator_address,
        'near' AS blockchain,
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
            'near_core',
            'dim_ft_contract_metadata'
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
    base
