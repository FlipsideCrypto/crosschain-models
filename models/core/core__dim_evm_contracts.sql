{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

{% set models = [ ('ethereum', source('ethereum_core', 'dim_contracts')), 
('polygon', source('polygon_core', 'dim_contracts')), 
('bsc', source('bsc_core', 'dim_contracts')), 
('avalanche', source('avalanche_core', 'dim_contracts')), 
('arbitrum', source('arbitrum_core', 'dim_contracts')),
('optimism', source('optimism_core', 'dim_contracts')) ] 
%}
SELECT 
    *
FROM (
    {% for models in models %}
    SELECT
        '{{ models[0] }}' AS blockchain,
        address,
        symbol,
        NAME,
        decimals  
    FROM {{ models[1] }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
