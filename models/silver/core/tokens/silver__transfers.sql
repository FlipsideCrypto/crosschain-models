{{ config(
    materialized = 'view',
    tags = ['daily']
) }}


WITH evm_transfers AS (
    {{ dbt_utils.union_relations(
        relations=[
            source('arbitrum_core', 'ez_token_transfers'),
            source('avalanche_core', 'ez_token_transfers'),
            source('base_core', 'ez_token_transfers'),
            source('bob_core', 'ez_token_transfers'),
            source('boba_core', 'ez_token_transfers'),
            source('bsc_core', 'ez_token_transfers'),
            source('core_core', 'ez_token_transfers'),
            source('ethereum_core', 'ez_token_transfers'),
            source('gnosis_core', 'ez_token_transfers'),
            source('ink_core', 'ez_token_transfers'),
            source('kaia_core', 'ez_token_transfers'),
            source('mantle_core', 'ez_token_transfers'),
            source('optimism_core', 'ez_token_transfers'),
            source('polygon_core', 'ez_token_transfers'),
            source('ronin_core', 'ez_token_transfers'),
            source('sei_evm_core', 'ez_token_transfers'),
            source('flow_evm_core', 'ez_token_transfers'),

            source('arbitrum_core', 'ez_native_transfers'),
            source('avalanche_core', 'ez_native_transfers'),
            source('base_core', 'ez_native_transfers'),
            source('bob_core', 'ez_native_transfers'),
            source('boba_core', 'ez_native_transfers'),
            source('bsc_core', 'ez_native_transfers'),
            source('core_core', 'ez_native_transfers'),
            source('ethereum_core', 'ez_native_transfers'),
            source('gnosis_core', 'ez_native_transfers'),
            source('ink_core', 'ez_native_transfers'),
            source('kaia_core', 'ez_native_transfers'),
            source('mantle_core', 'ez_native_transfers'),
            source('optimism_core', 'ez_native_transfers'),
            source('polygon_core', 'ez_native_transfers'),
            source('ronin_core', 'ez_native_transfers'),
            source('sei_evm_core', 'ez_native_transfers'),
            source('flow_evm_core', 'ez_native_transfers'),
        ] 
    ) }}
)
SELECT
    block_timestamp,
    contract_address AS address,
    LOWER(SPLIT_PART(_dbt_source_relation, '.', 1)) AS blockchain,
    tx_hash,
    from_address,
    to_address,
    amount,
    amount_usd,
    CASE WHEN contract_address is null then TRUE ELSE token_is_verified END AS token_is_verified,
    modified_timestamp
FROM
    evm_transfers
UNION ALL
    -- Non-EVM Chains
SELECT
    block_timestamp,
    token_address AS address,
    'aleo' AS blockchain,
    tx_id AS tx_hash,
    sender AS from_address,
    receiver AS to_address,
    amount,
    NULL AS amount_usd,
    FALSE AS token_is_verified,
    modified_timestamp
FROM
    {{ source(
        'aleo_core',
        'fact_transfers'
    ) }}
WHERE
    token_address IS NOT NULL
UNION ALL
SELECT
    block_timestamp,
    token_address AS address,
    'aptos' AS blockchain,
    tx_hash,
    CASE
        WHEN transfer_event = 'WithdrawEvent' THEN account_address
        ELSE NULL
    END AS from_address,
    CASE
        WHEN transfer_event = 'DepositEvent' THEN account_address
        ELSE NULL
    END AS to_address,
    amount,
    amount_usd,
    token_is_verified,
    modified_timestamp
FROM
    {{ source(
        'aptos_core',
        'ez_transfers'
    ) }}
UNION ALL
SELECT
    block_timestamp,
    currency AS address,
    'axelar' AS blockchain,
    tx_id AS tx_hash,
    sender AS from_address,
    receiver AS to_address,
    amount,
    NULL AS amount_usd,
    FALSE AS token_is_verified,
    modified_timestamp
FROM
    {{ source(
        'axelar_core',
        'fact_transfers'
    ) }}
UNION ALL
SELECT
    block_timestamp,
    currency AS address,
    'cosmos' AS blockchain,
    tx_id AS tx_hash,
    sender AS from_address,
    receiver AS to_address,
    amount,
    NULL AS amount_usd,
    FALSE AS token_is_verified,
    modified_timestamp
FROM
    {{ source(
        'cosmos_core',
        'fact_transfers'
    ) }}
UNION ALL
SELECT
    block_timestamp,
    mint AS address,
    'eclipse' AS blockchain,
    tx_id AS tx_hash,
    tx_from AS from_address,
    tx_to AS to_address,
    amount,
    NULL AS amount_usd,
    FALSE AS token_is_verified,
    modified_timestamp
FROM
    {{ source(
        'eclipse_core',
        'fact_transfers'
    ) }}
UNION ALL
SELECT
    block_timestamp,
    token_contract AS address,
    'flow' AS blockchain,
    tx_id AS tx_hash,
    sender AS from_address,
    recipient AS to_address,
    amount,
    NULL AS amount_usd,
    FALSE AS token_is_verified,
    modified_timestamp
FROM
    {{ source(
        'flow_core',
        'ez_token_transfers'
    ) }}
UNION ALL
SELECT
    block_timestamp,
    asset AS address,
    'maya' AS blockchain,
    fact_transfers_id AS tx_hash,
    from_address,
    to_address,
    cacao_amount AS amount,
    NULL AS amount_usd,
    FALSE AS token_is_verified,
    modified_timestamp
FROM
    {{ source(
        'maya_core',
        'fact_transfers'
    ) }}
UNION ALL
SELECT
    block_timestamp,
    contract_address AS address,
    'near' AS blockchain,
    tx_hash,
    from_address,
    to_address,
    amount,
    amount_usd,
    token_is_verified,
    modified_timestamp
FROM
    {{ source(
        'near_core',
        'ez_token_transfers'
    ) }}
UNION ALL
SELECT
    block_timestamp,
    currency AS address,
    'osmosis' AS blockchain,
    tx_id AS tx_hash,
    sender AS from_address,
    receiver AS to_address,
    amount,
    NULL AS amount_usd,
    FALSE AS token_is_verified,
    modified_timestamp
FROM
    {{ source(
        'osmosis_core',
        'fact_transfers'
    ) }}
UNION ALL
SELECT
    block_timestamp,
    mint AS address,
    'solana' AS blockchain,
    tx_id AS tx_hash,
    tx_from AS from_address,
    tx_to AS to_address,
    amount,
    amount_usd,
    token_is_verified,
    modified_timestamp
FROM
    {{ source(
        'solana_core',
        'ez_transfers'
    ) }}
UNION ALL
SELECT
    block_timestamp,
    UPPER(
        asset_issuer || '-' || asset_code
    ) AS address,
    'stellar' AS blockchain,
    transaction_id :: STRING AS tx_hash,
    from_account AS from_address,
    to_account AS to_address,
    amount,
    NULL AS amount_usd,
    FALSE AS token_is_verified,
    modified_timestamp
FROM
    {{ source(
        'stellar_core',
        'fact_operations'
    ) }}
WHERE
    amount IS NOT NULL
    AND type_string ILIKE '%payment%'
UNION ALL
SELECT
    block_timestamp,
    asset AS address,
    'thorchain' AS blockchain,
    fact_transfers_id AS tx_hash,
    from_address,
    to_address,
    rune_amount AS amount,
    NULL AS amount_usd,
    FALSE AS token_is_verified,
    modified_timestamp
FROM
    {{ source(
        'thorchain_core',
        'fact_transfers'
    ) }}
UNION ALL
SELECT
    block_timestamp,
    jetton_master AS address,
    'ton' AS blockchain,
    tx_hash,
    source AS from_address,
    destination AS to_address,
    amount,
    NULL AS amount_usd,
    FALSE AS token_is_verified,
    modified_timestamp
FROM
    {{ source(
        'ton_core',
        'fact_jetton_events'
    ) }}
WHERE
    TYPE = 'transfer'
