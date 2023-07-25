{{ config(
    materialized = 'view'
) }}



{% set models = [
    ('ethereum', source('ethereum_silver', 'abis')),
    ('polygon', source('polygon_silver', 'abis')),
    ('avalanche', source('avalanche_silver', 'abis')),
    ('bsc', source('bsc_silver', 'abis')),
    ('arbitrum', source('arbitrum_silver', 'abis')),
    ('optimism', source('optimism_silver', 'abis')),
    ('base', source('base_silver', 'abis')),
    ('gnosis', source('gnosis_silver', 'abis'))
]
%}

SELECT *
FROM (
        {% for models in models %}
        SELECT
        contract_address,
        abi_hash,
        '{{ models[0] }}' AS blockchain
        FROM {{ models[1] }}
        {% if not loop.last %}
        {% if is_incremental() %}
        {% endif %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )