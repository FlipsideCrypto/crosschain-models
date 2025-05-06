{{ config(
    materialized = 'view',
    tags = ['observability']
) }}

{% set models = [
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
    ('aleo', source('aleo_observ', 'transitions_completeness')),

    ('movement', source('movement_observ', 'blocks_completeness')),
    ('movement', source('movement_observ', 'transactions_completeness'))
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