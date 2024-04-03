{{ config(
    materialized = 'view'
) }}

{% set models = [
    ('ethereum', source('ethereum_silver', 'blocks')),
    ('polygon', source('polygon_silver', 'blocks')),
    ('avalanche', source('avalanche_silver', 'blocks')),
    ('bsc', source('bsc_silver', 'blocks')),
    ('arbitrum', source('arbitrum_silver', 'blocks')),
    ('optimism', source('optimism_silver', 'blocks')),
    ('base', source('base_silver', 'blocks')),
    ('gnosis', source('gnosis_silver', 'blocks')),
    ('blast', source('blast_silver', 'blocks'))
]
%}

SELECT *
FROM (
        {% for models in models %}
        SELECT
            '{{ models[0] }}' AS blockchain,
            block_timestamp::date as block_date,
            max(block_number) as block_number
        FROM {{ models[1] }}
            group by all 
        {% if not loop.last %}
        {% if is_incremental() %}
        {% endif %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )