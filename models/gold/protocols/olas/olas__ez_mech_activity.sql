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
    request_id,
    sender_address,
    prompt_link,
    delivery_link,
    ez_mech_activity_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ source(
        'gnosis_olas',
        'ez_mech_activity'
    ) }}
