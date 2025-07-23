{{ config(
    materialized = 'view',
    tags = ['daily']
) }}


WITH evm_events AS (
    {{ dbt_utils.union_relations(
        relations=[
            source('arbitrum_core', 'fact_event_logs'),
            source('avalanche_core', 'fact_event_logs'),
            source('base_core', 'fact_event_logs'),
            source('bob_core', 'fact_event_logs'),
            source('boba_core', 'fact_event_logs'),
            source('bsc_core', 'fact_event_logs'),
            source('core_core', 'fact_event_logs'),
            source('ethereum_core', 'fact_event_logs'),
            source('gnosis_core', 'fact_event_logs'),
            source('ink_core', 'fact_event_logs'),
            source('optimism_core', 'fact_event_logs'),
            source('polygon_core', 'fact_event_logs'),
            source('ronin_core', 'fact_event_logs'),
            source('sei_evm_core', 'fact_event_logs'),
            source('flow_evm_core', 'fact_event_logs') 
        ] 
    ) }}
)
SELECT
    block_timestamp,
    LOWER(SPLIT_PART(_dbt_source_relation, '.', 1)) AS blockchain,
    origin_from_address,
    contract_address,
    tx_hash,
    modified_timestamp 
FROM
    evm_events
UNION ALL
    -- Non-EVM Chains
SELECT
    block_timestamp,
    'aptos' AS blockchain,
    sender as origin_from_address,
    SPLIT_PART(t.payload_function, ':', 1) contract_address,
    tx_hash,
    modified_timestamp
FROM
      {{ source('aptos_core', 'fact_transactions') }}
WHERE
    contract_address IS NOT NULL
UNION ALL
SELECT
    block_timestamp,
    'near' AS blockchain,
    tx_signer as origin_from_address,
    tx_receiver as contract_address,
    tx_hash,
    modified_timestamp
FROM
      {{ source('near_core', 'fact_transactions') }} 
UNION ALL
SELECT
    block_timestamp,
    currency AS address,
    'axelar' AS blockchain,
    tx_id AS tx_hash,
    NULL AS origin_from_address,
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
    NULL AS origin_from_address,
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
    NULL AS origin_from_address,
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
    NULL AS origin_from_address,
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
    NULL AS origin_from_address,
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
    NULL AS origin_from_address,
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
    NULL AS origin_from_address,
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
    NULL AS origin_from_address,
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
    NULL AS origin_from_address,
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
    NULL AS origin_from_address,
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
    NULL AS origin_from_address,
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
