{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS',
    'PURPOSE': 'AI, AGENT, SERVICES',
    } } }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    event_name,
    owner_address,
    unit_id,
    u_type,
    unit_type,
    unit_hash,
    NAME,
    description,
    subcomponent_ids,
    trait_type,
    trait_value,
    image_link,
    unit_metadata_link,
    ez_unit_registrations_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ source(
        'ethereum_olas',
        'ez_unit_registrations'
    ) }}
