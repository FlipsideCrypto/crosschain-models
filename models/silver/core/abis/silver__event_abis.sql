{{ config(
    materialized = 'view',
    tags = ['daily']
) }}



{% set models = [ 
('ethereum', source('ethereum_silver', 'complete_event_abis')), 
('polygon', source('polygon_silver', 'complete_event_abis')) , 
('bsc', source('bsc_silver', 'complete_event_abis')) , 
('avalanche', source('avalanche_silver', 'complete_event_abis')) , 
('gnosis', source('gnosis_silver', 'complete_event_abis')) , 
('arbitrum', source('arbitrum_silver', 'complete_event_abis')),
('optimism', source('optimism_silver', 'complete_event_abis')),
('base', source('base_silver', 'complete_event_abis')),
('blast', source('blast_silver', 'complete_event_abis')),  
('kaia', source('kaia_silver', 'complete_event_abis')),
('mantle', source('mantle_silver', 'complete_event_abis')),
('core', source('core_silver', 'complete_event_abis')),
('sei', source('sei_evm_silver', 'complete_event_abis')),
('ronin', source('ronin_silver', 'complete_event_abis')),
('swell', source('swell_silver', 'complete_event_abis')),
('ink', source('ink_silver', 'complete_event_abis')),
('berachain-bartio', source('berachain_bartio_silver', 'complete_event_abis'))
 ] 
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