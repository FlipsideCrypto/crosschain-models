{{ config(
    materialized = 'view',
    tags = ['daily']
) }}

           {# source('bob_core', 'fact_event_logs'),
            source('boba_core', 'fact_event_logs'),
            source('core_core', 'fact_event_logs'),
            source('gnosis_core', 'fact_event_logs'),
            source('ink_core', 'fact_event_logs'),
                    source('ronin_core', 'fact_event_logs'),
            source('sei_evm_core', 'fact_event_logs'),
            source('flow_evm_core', 'fact_event_logs')  #}

WITH evm_events AS (
    {{ dbt_utils.union_relations(
        relations=[
            source('arbitrum_core', 'fact_event_logs'),
            source('avalanche_core', 'fact_event_logs'),
            source('base_core', 'fact_event_logs'),
 
            source('bsc_core', 'fact_event_logs'),
            
            source('ethereum_core', 'fact_event_logs'),

            source('optimism_core', 'fact_event_logs'),
            source('polygon_core', 'fact_event_logs'),
    
        ] 
    ) }}
)
SELECT
    block_timestamp,
    LOWER(SPLIT_PART(_dbt_source_relation, '.', 1)) AS blockchain,
    origin_from_address as sender,
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
    sender as sender,
    SPLIT_PART(payload_function, ':', 1) contract_address,
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
    tx_signer as sender,
    tx_receiver as contract_address,
    tx_hash,
    modified_timestamp
FROM
      {{ source('near_core', 'fact_transactions') }} 

UNION ALL
SELECT
    block_timestamp,
    'solana' AS blockchain,
    signers [0] :: STRING AS sender,
    e.program_id AS contract_address,
    e.tx_id AS tx_hash,
    modified_timestamp
FROM
    {{ source(
        'solana_core',
        'fact_events'
    ) }} e
WHERE
    e.succeeded = TRUE
    AND e.signers [0] IS NOT NULL