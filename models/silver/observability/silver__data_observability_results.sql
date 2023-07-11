{{ config(
    materialized = 'view'
) }}

{% set models = [ 
('avalanche', source('avalanche_observ', 'blocks_completeness')), 
('avalanche', source('avalanche_observ', 'logs_completeness')), 
('avalanche', source('avalanche_observ', 'receipts_completeness')), 
('avalanche', source('avalanche_observ', 'traces_completeness')), 
('avalanche', source('avalanche_observ', 'transactions_completeness')), 
('cosmos', source('cosmos_observ', 'blocks_completeness')), 
('cosmos', source('cosmos_observ', 'transactions_completeness')), 
('osmosis', source('osmosis_observ', 'blocks_completeness')),
('osmosis', source('osmosis_observ', 'transactions_completeness'))
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