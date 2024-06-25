{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS',
    'PURPOSE': 'AI, AGENT, SERVICES',
    } } }
) }}

SELECT
    'gnosis' AS blockchain,
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    event_name,
    sender_address,
    request_id,
    data_payload,
    prompt_link AS metadata_link,
    mech_requests_id AS fact_mech_activity_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ source(
        'gnosis_silver_olas',
        'mech_requests'
    ) }}
UNION ALL
SELECT
    'gnosis' AS blockchain,
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    event_name,
    sender_address,
    request_id,
    data_payload,
    delivery_link AS metadata_link,
    mech_delivers_id AS fact_mech_activity_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ source(
        'gnosis_silver_olas',
        'mech_delivers'
    ) }}
