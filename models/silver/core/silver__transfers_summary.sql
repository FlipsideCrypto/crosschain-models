{{ config(
    materialized = 'incremental',
    unique_key = ['transfers_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['blockchain','block_day'],
    merge_exclude_columns = ['inserted_timestamp'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address,blockchain);",
    tags = ['daily']
) }}

-- Set default date boundary
{% set default_date = "'2025-01-01'" %}

-- Simple incremental logic using fixed lookback period
{% if is_incremental() %}
    {% set block_ts_filter = "block_timestamp >= SYSDATE() - INTERVAL '1 day'" %}
    {% set max_mod = "SYSDATE() - INTERVAL '1 day'" %}
{% else %}
    {% set block_ts_filter = "block_timestamp >= " ~ default_date %}
    {% set max_mod = default_date %}
{% endif %}

WITH evm_transfers AS (
    {{ dbt_utils.union_relations(
        relations=[
            source('arbitrum_core', 'ez_token_transfers'),
            source('avalanche_core', 'ez_token_transfers'),
            source('base_core', 'ez_token_transfers'),
            source('blast_core', 'ez_token_transfers'),
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
            source('sei_evm_core', 'ez_token_transfers'),
            source('flow_evm_core', 'ez_token_transfers'),
        ],
        where=block_ts_filter
    ) }}
),

all_transfers AS (
    -- EVM Chains
    SELECT
        DATE(block_timestamp) as block_day,
        contract_address as address,
        LOWER(SPLIT_PART(_dbt_source_relation, '.', 1)) as blockchain,
        tx_hash,
        from_address,
        to_address,
        amount
    FROM 
        evm_transfers

    UNION ALL 

    -- Non-EVM Chains
    SELECT 
        DATE(block_timestamp) as block_day,
        token_address as address,
        'aleo' as blockchain,
        tx_id as tx_hash,
        sender as from_address,
        receiver as to_address,
        amount
    FROM {{ source('aleo_core', 'fact_transfers') }}
    WHERE {{ block_ts_filter }}
    AND token_address is not null
    
    UNION ALL 

    SELECT 
        DATE(block_timestamp) as block_day,
        token_address as address,
        'aptos' as blockchain,
        tx_hash,
        CASE 
            WHEN transfer_event = 'WithdrawEvent' THEN account_address 
            ELSE NULL 
        END as from_address,
        CASE 
            WHEN transfer_event = 'DepositEvent' THEN account_address
            ELSE NULL
        END as to_address,
        amount
    FROM {{ source('aptos_core', 'fact_transfers') }}
    WHERE {{ block_ts_filter }}

    UNION ALL

    SELECT 
        DATE(block_timestamp) as block_day,
        currency as address,
        'axelar' as blockchain,
        tx_id as tx_hash,
        sender as from_address,
        receiver as to_address,
        amount
    FROM {{ source('axelar_core', 'fact_transfers') }}
    WHERE {{ block_ts_filter }}

    UNION ALL

    SELECT 
        DATE(block_timestamp) as block_day,
        currency as address,
        'cosmos' as blockchain,
        tx_id as tx_hash,
        sender as from_address,
        receiver as to_address,
        amount
    FROM {{ source('cosmos_core', 'fact_transfers') }}
    WHERE {{ block_ts_filter }}

    UNION ALL

    SELECT 
        DATE(block_timestamp) as block_day,
        mint as address,
        'eclipse' as blockchain,
        tx_id as tx_hash,
        tx_from as from_address,
        tx_to as to_address,
        amount
    FROM {{ source('eclipse_core', 'fact_transfers') }}
    WHERE {{ block_ts_filter }}

    UNION ALL

    SELECT 
        DATE(block_timestamp) as block_day,
        token_contract as address,
        'flow' as blockchain,
        tx_id as tx_hash,
        sender as from_address,
        recipient as to_address,
        amount
    FROM {{ source('flow_core', 'ez_token_transfers') }}
    WHERE {{ block_ts_filter }}

    UNION ALL

    SELECT 
        DATE(block_timestamp) as block_day,
        contract_address as address,
        'near' as blockchain,
        tx_hash,
        from_address,
        to_address,
        amount
    FROM {{ source('near_core', 'ez_token_transfers') }}
    WHERE {{ block_ts_filter }}

    UNION ALL

    SELECT 
        DATE(block_timestamp) as block_day,
        currency as address,
        'osmosis' as blockchain,
        tx_id as tx_hash,
        sender as from_address,
        receiver as to_address,
        amount
    FROM {{ source('osmosis_core', 'fact_transfers') }}
    WHERE {{ block_ts_filter }}

    UNION ALL

    SELECT 
        DATE(block_timestamp) as block_day,
        mint as address,
        'solana' as blockchain,
        tx_id as tx_hash,
        tx_from as from_address,
        tx_to as to_address,
        amount
    FROM {{ source('solana_core', 'fact_transfers') }}
    WHERE {{ block_ts_filter }}

    {# UNION ALL --not really a contract address here
    SELECT 
        DATE(block_timestamp) as block_day,
        asset as address,
        'thorchain' as blockchain,
        fact_transfers_id as tx_hash,
        from_address,
        to_address,
        amount
    FROM {{ source('thorchain_core', 'fact_transfers') }}
    WHERE {{ block_ts_filter }} #}
   
),

aggregated_transfers AS (
    SELECT
        block_day,
        address,
        blockchain,
        count(distinct tx_hash) as tx_count,
        count(distinct from_address) as unique_senders,
        sum(amount) as amount
    FROM all_transfers
    WHERE address IS NOT NULL
    GROUP BY 1,2,3
    HAVING count(distinct tx_hash) >= 100 
        AND count(distinct from_address) >= 25
)

SELECT 
    block_day,
    address,
    blockchain,
    tx_count,
    unique_senders,
    amount,
    {{ dbt_utils.generate_surrogate_key(['address','blockchain','block_day']) }} AS transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id    
FROM 
    aggregated_transfers