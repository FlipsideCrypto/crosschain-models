{{ config(
    materialized = 'view'
) }}

{% set models = [
    ('avalanche', source('avalanche_observ', 'blocks_completeness')),
    ('avalanche', source('avalanche_observ', 'logs_completeness')),
    ('avalanche', source('avalanche_observ', 'receipts_completeness')),
    ('avalanche', source('avalanche_observ', 'traces_completeness')),
    ('avalanche', source('avalanche_observ', 'transactions_completeness')),
    ('ethereum', source('ethereum_observ', 'blocks_completeness')),
    ('ethereum', source('ethereum_observ', 'logs_completeness')),
    ('ethereum', source('ethereum_observ', 'receipts_completeness')),
    ('ethereum', source('ethereum_observ', 'traces_completeness')),
    ('ethereum', source('ethereum_observ', 'transactions_completeness')),
    ('arbitrum', source('arbitrum_observ', 'blocks_completeness')),
    ('arbitrum', source('arbitrum_observ', 'logs_completeness')),
    ('arbitrum', source('arbitrum_observ', 'receipts_completeness')),
    ('arbitrum', source('arbitrum_observ', 'traces_completeness')),
    ('arbitrum', source('arbitrum_observ', 'transactions_completeness')),
    ('bsc', source('bsc_observ', 'blocks_completeness')),
    ('bsc', source('bsc_observ', 'logs_completeness')),
    ('bsc', source('bsc_observ', 'receipts_completeness')),
    ('bsc', source('bsc_observ', 'traces_completeness')),
    ('bsc', source('bsc_observ', 'transactions_completeness')),
    ('polygon', source('polygon_observ', 'blocks_completeness')),
    ('polygon', source('polygon_observ', 'logs_completeness')),
    ('polygon', source('polygon_observ', 'receipts_completeness')),
    ('polygon', source('polygon_observ', 'traces_completeness')),
    ('polygon', source('polygon_observ', 'transactions_completeness')),
    ('optimism', source('optimism_observ', 'blocks_completeness')),
    ('optimism', source('optimism_observ', 'logs_completeness')),
    ('optimism', source('optimism_observ', 'receipts_completeness')),
    ('optimism', source('optimism_observ', 'traces_completeness')),
    ('optimism', source('optimism_observ', 'transactions_completeness')),
    ('base', source('base_observ', 'blocks_completeness')),
    ('base', source('base_observ', 'logs_completeness')),
    ('base', source('base_observ', 'receipts_completeness')),
    ('base', source('base_observ', 'traces_completeness')),
    ('base', source('base_observ', 'transactions_completeness')),
    ('gnosis', source('gnosis_observ', 'blocks_completeness')),
    ('gnosis', source('gnosis_observ', 'logs_completeness')),
    ('gnosis', source('gnosis_observ', 'receipts_completeness')),
    ('gnosis', source('gnosis_observ', 'traces_completeness')),
    ('gnosis', source('gnosis_observ', 'transactions_completeness')),
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
    ('solana', source('solana_observ', 'transactions_completeness'))
]
%}
SELECT *
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