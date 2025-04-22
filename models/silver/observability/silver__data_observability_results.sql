{{ config(
    materialized = 'view',
    tags = ['observability']
) }}

{% set models = [
    ('avalanche', source('avalanche_observ', 'blocks')),
    ('avalanche', source('avalanche_observ', 'logs')),
    ('avalanche', source('avalanche_observ', 'receipts')),
    ('avalanche', source('avalanche_observ', 'traces')),
    ('avalanche', source('avalanche_observ', 'transactions')),
    ('ethereum', source('ethereum_observ', 'blocks')),
    ('ethereum', source('ethereum_observ', 'logs')),
    ('ethereum', source('ethereum_observ', 'receipts')),
    ('ethereum', source('ethereum_observ', 'traces')),
    ('ethereum', source('ethereum_observ', 'transactions')),
    ('arbitrum', source('arbitrum_observ', 'blocks')),
    ('arbitrum', source('arbitrum_observ', 'logs')),
    ('arbitrum', source('arbitrum_observ', 'receipts')),
    ('arbitrum', source('arbitrum_observ', 'traces')),
    ('arbitrum', source('arbitrum_observ', 'transactions')),
    ('bsc', source('bsc_observ', 'blocks')),
    ('bsc', source('bsc_observ', 'logs')),
    ('bsc', source('bsc_observ', 'receipts')),
    ('bsc', source('bsc_observ', 'traces')),
    ('bsc', source('bsc_observ', 'transactions')),
    ('kaia', source('kaia_observ', 'blocks')),
    ('kaia', source('kaia_observ', 'logs')),
    ('kaia', source('kaia_observ', 'receipts')),
    ('kaia', source('kaia_observ', 'traces')),
    ('polygon', source('polygon_observ', 'blocks')),
    ('polygon', source('polygon_observ', 'logs')),
    ('polygon', source('polygon_observ', 'receipts')),
    ('polygon', source('polygon_observ', 'traces')),
    ('polygon', source('polygon_observ', 'transactions')),
    ('optimism', source('optimism_observ', 'blocks')),
    ('optimism', source('optimism_observ', 'logs')),
    ('optimism', source('optimism_observ', 'receipts')),
    ('optimism', source('optimism_observ', 'traces')),
    ('optimism', source('optimism_observ', 'transactions')),
    ('base', source('base_observ', 'blocks')),
    ('base', source('base_observ', 'logs')),
    ('base', source('base_observ', 'receipts')),
    ('base', source('base_observ', 'traces')),
    ('base', source('base_observ', 'transactions')),
    ('gnosis', source('gnosis_observ', 'blocks')),
    ('gnosis', source('gnosis_observ', 'logs')),
    ('gnosis', source('gnosis_observ', 'receipts')),
    ('gnosis', source('gnosis_observ', 'traces')),
    ('gnosis', source('gnosis_observ', 'transactions')),
    ('blast', source('blast_observ', 'blocks')),
    ('blast', source('blast_observ', 'logs')),
    ('blast', source('blast_observ', 'receipts')),
    ('blast', source('blast_observ', 'traces')),
    ('blast', source('blast_observ', 'transactions')),
    ('cosmos', source('cosmos_observ', 'blocks_completeness')),
    ('cosmos', source('cosmos_observ', 'transactions_completeness')),
    ('osmosis', source('osmosis_observ', 'blocks_completeness')),
    ('osmosis', source('osmosis_observ', 'transactions_completeness')),
    ('axelar', source('axelar_observ', 'blocks_completeness')),
    ('axelar', source('axelar_observ', 'transactions_completeness')),
    ('terra', source('terra_observ', 'blocks_completeness')),
    ('terra', source('terra_observ', 'transactions_completeness')),
    ('sei', source('sei_observ', 'blocks_completeness')),
    ('sei', source('sei_observ', 'transactions_completeness')),

    ('aurora', source('aurora_observ', 'blocks_completeness')),
    ('aurora', source('aurora_observ', 'logs_completeness')),
    ('aurora', source('aurora_observ', 'receipts_completeness')),
    ('aurora', source('aurora_observ', 'transactions_completeness')),

    ('bitcoin', source('bitcoin_observ', 'blocks_completeness')),
    ('bitcoin', source('bitcoin_observ', 'inputs_completeness')),
    ('bitcoin', source('bitcoin_observ', 'outputs_completeness')),
    ('bitcoin', source('bitcoin_observ', 'txs_completeness')),

    ('flow', source('flow_observ', 'blocks_completeness')),
    ('flow', source('flow_observ', 'txs_completeness')),

    ('near', source('near_observ', 'blocks_completeness')),
    ('near', source('near_observ', 'chunks_completeness')),
    ('near', source('near_observ', 'logs_completeness')),

    ('solana', source('solana_observ', 'blocks_completeness')),
    ('solana', source('solana_observ', 'transactions_completeness')),

    ('aptos', source('aptos_observ', 'blocks_completeness')),
    ('aptos', source('aptos_observ', 'transactions_completeness')),

    ('lava', source('lava_observ', 'blocks_completeness')),
    ('lava', source('lava_observ', 'transactions_completeness')),
  
    ('aleo', source('aleo_observ', 'blocks_completeness')),
    ('aleo', source('aleo_observ', 'transactions_completeness')),
    ('aleo', source('aleo_observ', 'transitions_completeness'))
]
%}
SELECT 
    *,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain','test_name','test_timestamp']) }} AS data_observability_result_id,
    '{{ invocation_id }}' as _invocation_id
FROM (
        {% for models in models %}
        SELECT
        '{{ models[0] }}' AS blockchain,
        test_name,
        min_block,
        max_block,
        min_block_timestamp,
        max_block_timestamp,
        blocks_tested,
        blocks_impacted_count,
        blocks_impacted_array,
        test_timestamp
        FROM {{ models[1] }}
        {% if not loop.last %}
        {% if is_incremental() %}
        {% endif %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )