{{ config(
    materialized = 'view',
    tags = ['daily']
) }}

WITH base AS (

    SELECT
        tx_id AS tx_hash,
        block_timestamp,
        tx_succeeded,
        fee_payer AS sender,
        fee AS fee_native,
        modified_timestamp,
        'aleo' AS blockchain
    FROM
        {{ source(
            'aleo_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        success AS tx_succeeded,
        sender,
        COALESCE(
            gas_unit_price,
            0
        ) * gas_used AS fee_native,
        modified_timestamp,
        'aptos' AS blockchain
    FROM
        {{ source(
            'aptos_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'arbitrum' AS blockchain
    FROM
        {{ source(
            'arbitrum_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'avalanche' AS blockchain
    FROM
        {{ source(
            'avalanche_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_id AS tx_hash,
        block_timestamp,
        tx_succeeded,
        tx_from AS sender,
        fee / pow(
            10,
            6
        ) AS fee_native,
        modified_timestamp,
        'axelar' AS blockchain
    FROM
        {{ source(
            'axelar_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'base' AS blockchain
    FROM
        {{ source(
            'base_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'blast' AS blockchain
    FROM
        {{ source(
            'blast_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'bob' AS blockchain
    FROM
        {{ source(
            'bob_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'boba' AS blockchain
    FROM
        {{ source(
            'boba_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'bsc' AS blockchain
    FROM
        {{ source(
            'bsc_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_id AS tx_hash,
        block_timestamp,
        tx_succeeded,
        tx_from AS sender,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    fee,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) / pow(
            10,
            6
        ) AS fee_native,
        modified_timestamp,
        'cosmos' AS blockchain
    FROM
        {{ source(
            'cosmos_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'core' AS blockchain
    FROM
        {{ source(
            'core_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_id AS tx_hash,
        block_timestamp,
        succeeded AS tx_succeeded,
        signers [0] AS sender,
        fee / pow(
            10,
            9
        ) AS fee_native,
        modified_timestamp,
        'eclipse' AS blockchain
    FROM
        {{ source(
            'eclipse_core',
            'fact_transactions'
        ) }}
    UNION ALL
        {# SELECT
        tx_id AS tx_hash,
        block_timestamp,
        succeeded AS tx_succeeded,
        signers [0] AS sender,
        fee / pow(
            10,
            9
        ) AS fee_native,
        modified_timestamp,
        'eclipse' AS blockchain
    FROM
        {{ source(
            'eclipse_gov',
            'fact_votes'
        ) }}
    UNION ALL
        #}
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'ethereum' AS blockchain
    FROM
        {{ source(
            'ethereum_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'flow' AS blockchain
    FROM
        {{ source(
            'flow_evm_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        sender,
        fee_native,
        modified_timestamp,
        'flow' AS blockchain
    FROM
        {{ ref('silver__fact_transactions_flow') }} A
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'gnosis' AS blockchain
    FROM
        {{ source(
            'gnosis_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'ink' AS blockchain
    FROM
        {{ source(
            'ink_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'kaia' AS blockchain
    FROM
        {{ source(
            'kaia_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'mantle' AS blockchain
    FROM
        {{ source(
            'mantle_core',
            'fact_transactions'
        ) }}
        {# UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'maya' AS blockchain
    FROM
        {{ source(
            'maya_core',
            'fact_transfers'
        ) }} A #}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        tx_signer AS sender,
        transaction_fee / pow(
            10,
            24
        ) AS fee_native,
        modified_timestamp,
        'near' AS blockchain
    FROM
        {{ source(
            'near_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'optimism' AS blockchain
    FROM
        {{ source(
            'optimism_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'polygon' AS blockchain
    FROM
        {{ source(
            'polygon_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'ronin' AS blockchain
    FROM
        {{ source(
            'ronin_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'sei' AS blockchain
    FROM
        {{ source(
            'sei_evm_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_id AS tx_hash,
        block_timestamp,
        tx_succeeded,
        tx_from AS sender,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    fee,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) / pow(
            10,
            6
        ) AS fee_native,
        modified_timestamp,
        'sei' AS blockchain
    FROM
        {{ source(
            'sei_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_id AS tx_hash,
        block_timestamp,
        succeeded AS tx_succeeded,
        signers [0] AS sender,
        fee / pow(
            10,
            9
        ) AS fee_native,
        modified_timestamp,
        'solana' AS blockchain
    FROM
        {{ source(
            'solana_core',
            'fact_transactions'
        ) }}
    UNION ALL
        {# SELECT
        tx_id AS tx_hash,
        block_timestamp,
        succeeded AS tx_succeeded,
        signers [0] AS sender,
        fee / pow(
            10,
            9
        ) AS fee_native,
        modified_timestamp,
        'solana' AS blockchain
    FROM
        {{ source(
            'solana_gov',
            'fact_votes'
        ) }}
    UNION ALL
        #}
    SELECT
        transaction_hash AS tx_hash,
        block_timestamp,
        SUCCESSFUL AS tx_succeeded,
        account AS sender,
        fee_charged / pow(
            10,
            7
        ) AS fee_native,
        modified_timestamp,
        'stellar' AS blockchain
    FROM
        {{ source(
            'stellar_core',
            'fact_transactions'
        ) }}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        from_address AS sender,
        tx_fee_precise AS fee_native,
        modified_timestamp,
        'swell' AS blockchain
    FROM
        {{ source(
            'swell_core',
            'fact_transactions'
        ) }}
        {# UNION ALL
    SELECT
        'thorchain' AS blockchain
    FROM
        {{ source(
            'thorchain_core',
            'fact_transactions'
        ) }}
        #}
    UNION ALL
    SELECT
        tx_hash,
        block_timestamp,
        tx_succeeded,
        account AS sender,
        total_fees / pow(
            10,
            9
        ) AS fee_native,
        modified_timestamp,
        'ton' AS blockchain
    FROM
        {{ source(
            'ton_core',
            'fact_transactions'
        ) }}
)
SELECT
    tx_hash,
    block_timestamp,
    tx_succeeded,
    sender,
    fee_native,
    modified_timestamp,
    blockchain,
    modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain','tx_hash']) }} AS fact_transactions_lite_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
WHERE
    block_timestamp :: DATE >= '2025-01-01'
