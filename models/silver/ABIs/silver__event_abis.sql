{{ config(
    materialized = 'view'
) }}



{% set models = [ ('ethereum', source('ethereum_silver', 'complete_event_abis')), 
('polygon', source('polygon_silver', 'complete_event_abis')) , 
('bsc', source('bsc_silver', 'complete_event_abis')) , 
('avalanche', source('avalanche_silver', 'complete_event_abis')) , 
('gnosis', source('gnosis_silver', 'complete_event_abis')) , 
('arbitrum', source('arbitrum_silver', 'complete_event_abis')),
('optimism', source('optimism_silver', 'complete_event_abis')),
('base', source('base_silver', 'complete_event_abis')),
('blast', source('blast_silver', 'complete_event_abis')) ] 
%}
SELECT *
FROM (
        {% for models in models %}
        SELECT
        '{{ models[0] }}' AS blockchain,
        parent_contract_address,
        event_name,
        abi,
        simple_event_name,
        event_signature,
        start_block,
        end_block
        FROM {{ models[1] }}
        {% if not loop.last %}
        {% if is_incremental() %}
        {% endif %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )