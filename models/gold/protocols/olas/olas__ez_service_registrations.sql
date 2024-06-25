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
        service_registration_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'ethereum_silver_olas',
            'service_registrations'
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
        service_registration_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'gnosis_silver_olas',
            'service_registrations'
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
        service_registration_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'arbitrum_silver_olas',
            'service_registrations'
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
        service_registration_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'base_silver_olas',
            'service_registrations'
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
        service_registration_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'optimism_silver_olas',
            'service_registrations'
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
        service_registration_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'polygon_silver_olas',
            'service_registrations'
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
    r.blockchain,
    r.block_number,
    r.block_timestamp,
    r.tx_hash,
    r.origin_function_signature,
    r.origin_from_address,
    r.origin_to_address,
    r.contract_address,
    r.event_index,
    r.event_name,
    r.owner_address,
    r.multisig_address,
    r.service_id,
    m.name,
    m.description,
    m.agent_ids,
    m.trait_type,
    m.trait_value,
    m.image_link,
    m.code_uri_link AS service_metadata_link,
    {{ dbt_utils.generate_surrogate_key(
        ['r.registry_metadata_id','r.blockchain']
    ) }} AS ez_service_registrations_id,
    r.inserted_timestamp,
    GREATEST(
        COALESCE(
            r.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        ),
        COALESCE(
            m.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        )
    ) AS modified_timestamp
FROM
    all_registrations r
    LEFT JOIN {{ ref('olas__dim_registry_metadata') }}
    m
    ON r.contract_address = m.contract_address
    AND r.service_id = m.registry_id
    AND r.blockchain = m.blockchain
