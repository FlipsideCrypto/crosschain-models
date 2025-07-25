{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

{% set models = [ 
('ethereum', source('ethereum_silver', 'complete_event_abis')), 
('polygon', source('polygon_silver', 'complete_event_abis')) , 
('bsc', source('bsc_silver', 'complete_event_abis')) , 
('avalanche', source('avalanche_silver', 'complete_event_abis')) , 
('gnosis', source('gnosis_silver', 'complete_event_abis')) , 
('arbitrum', source('arbitrum_silver', 'complete_event_abis')),
('base', source('base_silver', 'complete_event_abis')),
('blast', source('blast_silver', 'complete_event_abis')),
('kaia', source('kaia_silver', 'complete_event_abis')),
('optimism', source('optimism_silver', 'complete_event_abis')),
('core', source('core_silver', 'complete_event_abis')),
('sei', source('sei_evm_silver', 'complete_event_abis')),
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
            end_block,
            COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
            COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
            {{ dbt_utils.generate_surrogate_key(['parent_contract_address','event_signature','start_block','blockchain']) }} AS dim_evm_event_abis_id
        FROM {{ models[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)