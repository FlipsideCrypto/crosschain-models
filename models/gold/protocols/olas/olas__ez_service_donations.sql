{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS',
    'PURPOSE': 'AI, AGENT, SERVICES',
    } } }
) }}

SELECT
    'ethereum' AS blockchain,
    s.block_number,
    s.block_timestamp,
    s.tx_hash,
    s.origin_function_signature,
    s.origin_from_address,
    s.origin_to_address,
    s.contract_address,
    s.event_index,
    s.event_name,
    s.donor_address,
    s.service_id,
    m.name,
    m.description,
    m.agent_ids,
    s.eth_amount_unadj,
    s.eth_amount,
    s.eth_amount_usd,
    s.service_donations_id AS ez_service_donations_id,
    s.inserted_timestamp,
    GREATEST(
        COALESCE(
            s.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        ),
        COALESCE(
            m.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        )
    ) AS modified_timestamp
FROM
    {{ source(
        'ethereum_silver_olas',
        'service_donations'
    ) }}
    s
    LEFT JOIN {{ source(
        'ethereum_silver_olas',
        'registry_metadata_complete'
    ) }}
    m
    ON m.contract_address = '0x48b6af7b12c71f09e2fc8af4855de4ff54e775ca'
    AND s.service_id = m.registry_id
