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
        registry_metadata_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'ethereum_silver_olas',
            'registry_metadata_complete'
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
        registry_metadata_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'gnosis_silver_olas',
            'registry_metadata_complete'
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
        registry_metadata_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'arbitrum_silver_olas',
            'registry_metadata_complete'
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
        registry_metadata_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'base_silver_olas',
            'registry_metadata_complete'
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
        registry_metadata_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'optimism_silver_olas',
            'registry_metadata_complete'
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
        registry_metadata_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'polygon_silver_olas',
            'registry_metadata_complete'
        ) }}
),
solana AS (
    SELECT
        'solana' AS blockchain,
        NAME,
        description,
        registry_id,
        program_id as contract_address,
        registry_type,
        trait_type,
        trait_value,
        code_uri_link,
        image_link,
        agent_ids,
        NULL AS subcomponent_ids,
        registry_metadata_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'solana_silver_olas',
            'registry_metadata_complete'
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
    UNION ALL
    SELECT
        *
    FROM
        solana
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
    {{ dbt_utils.generate_surrogate_key(
        ['registry_metadata_id','blockchain']
    ) }} AS dim_registry_metadata_id,
    inserted_timestamp,
    modified_timestamp
FROM
    all_metadata
