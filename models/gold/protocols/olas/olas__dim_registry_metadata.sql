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
        NAME,
        description,
        registry_id,
        contract_address,
        registry_type,
        trait_type,
        trait_value,
        code_uri_link,
        image_link,
        agent_ids,
        subcomponent_ids,
        dim_registry_metadata_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'ethereum_olas',
            'dim_registry_metadata'
        ) }}
),
gnosis AS (
    SELECT
        'gnosis' AS blockchain,
        NAME,
        description,
        registry_id,
        contract_address,
        registry_type,
        trait_type,
        trait_value,
        code_uri_link,
        image_link,
        agent_ids,
        NULL AS subcomponent_ids,
        dim_registry_metadata_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'gnosis_olas',
            'dim_registry_metadata'
        ) }}
),
arbitrum AS (
    SELECT
        'arbitrum' AS blockchain,
        NAME,
        description,
        registry_id,
        contract_address,
        registry_type,
        trait_type,
        trait_value,
        code_uri_link,
        image_link,
        agent_ids,
        NULL AS subcomponent_ids,
        dim_registry_metadata_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'arbitrum_olas',
            'dim_registry_metadata'
        ) }}
),
base AS (
    SELECT
        'base' AS blockchain,
        NAME,
        description,
        registry_id,
        contract_address,
        registry_type,
        trait_type,
        trait_value,
        code_uri_link,
        image_link,
        agent_ids,
        NULL AS subcomponent_ids,
        dim_registry_metadata_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'base_olas',
            'dim_registry_metadata'
        ) }}
),
optimism AS (
    SELECT
        'optimism' AS blockchain,
        NAME,
        description,
        registry_id,
        contract_address,
        registry_type,
        trait_type,
        trait_value,
        code_uri_link,
        image_link,
        agent_ids,
        NULL AS subcomponent_ids,
        dim_registry_metadata_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'optimism_olas',
            'dim_registry_metadata'
        ) }}
),
polygon AS (
    SELECT
        'polygon' AS blockchain,
        NAME,
        description,
        registry_id,
        contract_address,
        registry_type,
        trait_type,
        trait_value,
        code_uri_link,
        image_link,
        agent_ids,
        NULL AS subcomponent_ids,
        dim_registry_metadata_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'polygon_olas',
            'dim_registry_metadata'
        ) }}
),
all_metadata AS (
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
    NAME,
    description,
    registry_id,
    contract_address,
    registry_type,
    trait_type,
    trait_value,
    code_uri_link,
    image_link,
    agent_ids,
    subcomponent_ids,
    dim_registry_metadata_id,
    inserted_timestamp,
    modified_timestamp
FROM
    all_metadata
