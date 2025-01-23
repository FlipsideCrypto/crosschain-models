{{ config(
    materialized = 'view',
    tags = ['abis']
) }}



{% set models = [
    ('ethereum', source('ethereum_silver', 'abis')),
    ('polygon', source('polygon_silver', 'abis')),
    ('avalanche', source('avalanche_silver', 'abis')),
    ('bsc', source('bsc_silver', 'abis')),
    ('arbitrum', source('arbitrum_silver', 'abis')),
    ('optimism', source('optimism_silver', 'abis')),
    ('base', source('base_silver', 'abis')),
    ('gnosis', source('gnosis_silver', 'abis')),
    ('blast', source('blast_silver', 'abis')),
    ('sei', source('sei_evm_silver', 'abis')),
    ('kaia', source('kaia_silver', 'abis')),
    ('berachain-bartio', source('berachain_bartio_silver', 'abis')),
    ('mantle', source('mantle_silver', 'abis')),
    ('ronin', source('ronin_silver', 'abis')),
    ('core', source('core_silver', 'abis'))
]
%}

SELECT *
FROM (
        {% for models in models %}
        SELECT
        contract_address,
        abi_hash,
        '{{ models[0] }}' AS blockchain,
        coalesce(inserted_timestamp,'2001-01-01'::timestamp_ntz) as inserted_timestamp,
        coalesce(modified_timestamp,'2001-01-01'::timestamp_ntz) as modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['blockchain','contract_address']) }} as abis_id,
        _invocation_id
        FROM {{ models[1] }}
        {% if not loop.last %}
        {% if is_incremental() %}
        {% endif %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )