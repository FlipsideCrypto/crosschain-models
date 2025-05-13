{{ config(
    materialized = 'incremental',
    unique_key = ['transfers_id'],
    cluster_by = ['day_'],
    merge_exclude_columns = ['inserted_timestamp'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address,symbol);",
    tags = ['daily']
) }}

{% if execute %}
{% if is_incremental() %}
{% set max_mod_query %}
SELECT
    MAX(modified_timestamp) :: DATE AS modified_timestamp
FROM
    {{ this }}
{% endset %}
{% set max_mod = run_query(max_mod_query) [0] [0] %}
{% endif %}
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
        where="block_timestamp >= '2025-04-01' and modified_timestamp :: DATE >= '" ~ max_mod ~ "'"
    ) }}
),

all_transfers AS (
    -- EVM Chains
    SELECT
        DATE(block_timestamp) as day_,
        contract_address as address,
        LOWER(SPLIT_PART(_dbt_source_relation, '.', 1)) as blockchain,
        symbol,
        decimals,
        name,
        tx_hash,
        from_address,
        to_address,
        amount
    FROM 
        evm_transfers

    UNION ALL 

    -- Non-EVM Chains
    SELECT 
        DATE(block_timestamp) as day_,
        token_address as address,
        'aleo' as blockchain,
        NULL as symbol,
        NULL as decimals,
        NULL as name,
        tx_id as tx_hash,
        sender as from_address,
        receiver as to_address,
        amount
    FROM {{ source('aleo_core', 'fact_transfers') }}
    WHERE block_timestamp >= '2025-04-01'
    {% if is_incremental() %}
    AND modified_timestamp :: DATE >= '{{ max_mod }}'
    {% endif %}

    UNION ALL 

    SELECT 
        DATE(block_timestamp) as day_,
        token_address as address,
        'aptos' as blockchain,
        NULL as symbol,
        NULL as decimals,
        NULL as name,
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
    WHERE block_timestamp >= '2025-04-01'
    {% if is_incremental() %}
    AND modified_timestamp :: DATE >= '{{ max_mod }}'
    {% endif %}

    UNION ALL

    SELECT 
        DATE(block_timestamp) as day_,
        currency as address,
        'axelar' as blockchain,
        NULL as symbol,
        decimal as decimals,
        NULL as name,
        tx_id as tx_hash,
        sender as from_address,
        receiver as to_address,
        amount
    FROM {{ source('axelar_core', 'fact_transfers') }}
    WHERE block_timestamp >= '2025-04-01'
    {% if is_incremental() %}
    AND modified_timestamp :: DATE >= '{{ max_mod }}'
    {% endif %}

    UNION ALL

    SELECT 
        DATE(block_timestamp) as day_,
        currency as address,
        'cosmos' as blockchain,
        NULL as symbol,
        NULL as decimals,
        NULL as name,
        tx_id as tx_hash,
        sender as from_address,
        receiver as to_address,
        amount
    FROM {{ source('cosmos_core', 'fact_transfers') }}
    WHERE block_timestamp >= '2025-04-01'
    {% if is_incremental() %}
    AND modified_timestamp :: DATE >= '{{ max_mod }}'
    {% endif %}

    UNION ALL

    SELECT 
        DATE(block_timestamp) as day_,
        tx_from as address,
        'eclipse' as blockchain,
        NULL as symbol,
        NULL as decimals,
        NULL as name,
        tx_id as tx_hash,
        tx_from as from_address,
        tx_to as to_address,
        amount
    FROM {{ source('eclipse_core', 'fact_transfers') }}
    WHERE block_timestamp >= '2025-04-01'
    {% if is_incremental() %}
    AND modified_timestamp :: DATE >= '{{ max_mod }}'
    {% endif %}

    UNION ALL

    SELECT 
        DATE(block_timestamp) as day_,
        token_contract as address,
        'flow' as blockchain,
        NULL as symbol,
        NULL as decimals,
        NULL as name,
        tx_id as tx_hash,
        sender as from_address,
        recipient as to_address,
        amount
    FROM {{ source('flow_core', 'ez_token_transfers') }}
    WHERE block_timestamp >= '2025-04-01'
    {% if is_incremental() %}
    AND modified_timestamp :: DATE >= '{{ max_mod }}'
    {% endif %}

    UNION ALL

    SELECT 
        DATE(block_timestamp) as day_,
        contract_address as address,
        'near' as blockchain,
        NULL as symbol,
        NULL as decimals,
        NULL as name,
        tx_hash,
        from_address,
        to_address,
        amount
    FROM {{ source('near_core', 'ez_token_transfers') }}
    WHERE block_timestamp >= '2025-04-01'
    {% if is_incremental() %}
    AND modified_timestamp :: DATE >= '{{ max_mod }}'
    {% endif %}

    UNION ALL

    SELECT 
        DATE(block_timestamp) as day_,
        currency as address,
        'osmosis' as blockchain,
        NULL as symbol,
        decimal as decimals,
        NULL as name,
        tx_id as tx_hash,
        sender as from_address,
        receiver as to_address,
        amount
    FROM {{ source('osmosis_core', 'fact_transfers') }}
    WHERE block_timestamp >= '2025-04-01'
    {% if is_incremental() %}
    AND modified_timestamp :: DATE >= '{{ max_mod }}'
    {% endif %}

    UNION ALL

    SELECT 
        DATE(block_timestamp) as day_,
        mint as address,
        'solana' as blockchain,
        NULL as symbol,
        NULL as decimals,
        NULL as name,
        tx_id as tx_hash,
        tx_from as from_address,
        tx_to as to_address,
        amount
    FROM {{ source('solana_core', 'fact_transfers') }}
    WHERE block_timestamp >= '2025-04-01'
    {% if is_incremental() %}
    AND modified_timestamp :: DATE >= '{{ max_mod }}'
    {% endif %}

    {# Commented out thorchain
    UNION ALL
    SELECT 
        DATE(block_timestamp) as day_,
        asset as address,
        'thorchain' as blockchain,
        NULL as symbol,
        NULL as decimals,
        NULL as name,
        tx_id as tx_hash,
        tx_from as from_address,
        tx_to as to_address,
        amount
    FROM {{ source('thorchain_core', 'fact_transfers') }}
    WHERE block_timestamp >= '2025-04-01'
    {% if is_incremental() %}
    AND modified_timestamp :: DATE >= '{{ max_mod }}'
    {% endif %}
    #}
),

aggregated_transfers AS (
    SELECT
        day_,
        a.address,
        a.blockchain,
        COALESCE(a.symbol, t.symbol) as symbol,
        COALESCE(a.decimals, t.decimals) as decimals,
        COALESCE(a.name, t.name) as name,
        count(distinct tx_hash) as tx_count,
        count(distinct from_address) as unique_senders,
        sum(amount) as amount
    FROM all_transfers a
    LEFT JOIN {{ ref('silver__tokens') }} t
        ON a.address = t.address
        AND a.blockchain = t.blockchain
    GROUP BY 1,2,3,4,5,6
    HAVING count(distinct tx_hash) >= 100
        AND count(distinct from_address) >= 1000
)

SELECT 
    day_,
    address,
    blockchain,
    symbol,
    decimals,
    name,
    tx_count,
    unique_senders,
    amount,
    {{ dbt_utils.generate_surrogate_key(['address','blockchain','day_']) }} AS transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id    
FROM 
    aggregated_transfers