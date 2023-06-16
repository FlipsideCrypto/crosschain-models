{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

{% set models = [ ('ethereum', source('ethereum_silver', 'contracts')), 
('polygon', source('polygon_silver', 'contracts')) , 
('bsc', source('bsc_silver', 'contracts')) , 
('avalanche', source('avalanche_silver', 'contracts')) , 
('gnosis', source('gnosis_silver', 'contracts')) , 
('arbitrum', source('arbitrum_silver', 'contracts')),
('optimism', source('optimism_silver', 'contracts')) ] 
%}
SELECT 
    blockchain,
    contract_address AS address,
    token_symbol AS symbol,
    token_name AS NAME,
    token_decimals AS decimals  
FROM (
    {% for models in models %}
    SELECT
        '{{ models[0] }}' AS blockchain,
        contract_address,
        token_symbol,
        token_name,
        token_decimals
    FROM {{ models[1] }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
