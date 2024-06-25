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
        multisig_address,
        service_id,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        segmented_data,
        service_event_logs_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'ethereum_silver_olas',
            'service_event_logs'
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
        multisig_address,
        service_id,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        segmented_data,
        service_event_logs_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'gnosis_silver_olas',
            'service_event_logs'
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
        multisig_address,
        service_id,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        segmented_data,
        service_event_logs_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'arbitrum_silver_olas',
            'service_event_logs'
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
        multisig_address,
        service_id,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        segmented_data,
        service_event_logs_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'base_silver_olas',
            'service_event_logs'
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
        multisig_address,
        service_id,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        segmented_data,
        service_event_logs_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'optimism_silver_olas',
            'service_event_logs'
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
        multisig_address,
        service_id,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        segmented_data,
        service_event_logs_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ source(
            'polygon_silver_olas',
            'service_event_logs'
        ) }}
),
all_logs AS (
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
    multisig_address,
    service_id,
    topic_0,
    topic_1,
    topic_2,
    topic_3,
    DATA,
    segmented_data,
    {{ dbt_utils.generate_surrogate_key(
        ['service_event_logs_id','blockchain']
    ) }} AS fact_service_event_logs_id,
    inserted_timestamp,
    modified_timestamp
FROM
    all_logs
