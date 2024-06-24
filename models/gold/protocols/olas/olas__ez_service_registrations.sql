{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS',
    'PURPOSE': 'AI, AGENT, SERVICES',
    } } }
) }}

WITH ethereum AS (

    SELECT
        'ethereum' AS blockchain,
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
        multisig_address,
        service_id,
        NAME,
        description,
        agent_ids,
        trait_type,
        trait_value,
        image_link,
        service_metadata_link,
        ez_service_registrations_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'ethereum_olas',
            'ez_service_registrations'
        ) }}
),
gnosis AS (
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
        owner_address,
        multisig_address,
        service_id,
        NAME,
        description,
        agent_ids,
        trait_type,
        trait_value,
        image_link,
        service_metadata_link,
        ez_service_registrations_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'gnosis_olas',
            'ez_service_registrations'
        ) }}
),
arbitrum AS (
    SELECT
        'arbitrum' AS blockchain,
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
        multisig_address,
        service_id,
        NAME,
        description,
        agent_ids,
        trait_type,
        trait_value,
        image_link,
        service_metadata_link,
        ez_service_registrations_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'arbitrum_olas',
            'ez_service_registrations'
        ) }}
),
base AS (
    SELECT
        'base' AS blockchain,
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
        multisig_address,
        service_id,
        NAME,
        description,
        agent_ids,
        trait_type,
        trait_value,
        image_link,
        service_metadata_link,
        ez_service_registrations_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'base_olas',
            'ez_service_registrations'
        ) }}
),
optimism AS (
    SELECT
        'optimism' AS blockchain,
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
        multisig_address,
        service_id,
        NAME,
        description,
        agent_ids,
        trait_type,
        trait_value,
        image_link,
        service_metadata_link,
        ez_service_registrations_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'optimism_olas',
            'ez_service_registrations'
        ) }}
),
polygon AS (
    SELECT
        'polygon' AS blockchain,
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
        multisig_address,
        service_id,
        NAME,
        description,
        agent_ids,
        trait_type,
        trait_value,
        image_link,
        service_metadata_link,
        ez_service_registrations_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'polygon_olas',
            'ez_service_registrations'
        ) }}
),
all_registrations AS (
    SELECT
        *
    FROM
        ethereum
    UNION ALL
    SELECT
        *
    FROM
        gnosis
    UNION ALL
    SELECT
        *
    FROM
        arbitrum
    UNION ALL
    SELECT
        *
    FROM
        base
    UNION ALL
    SELECT
        *
    FROM
        optimism
    UNION ALL
    SELECT
        *
    FROM
        polygon
)
SELECT
    blockchain,
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
    multisig_address,
    service_id,
    NAME,
    description,
    agent_ids,
    trait_type,
    trait_value,
    image_link,
    service_metadata_link,
    ez_service_registrations_id,
    inserted_timestamp,
    modified_timestamp
FROM
    all_registrations
