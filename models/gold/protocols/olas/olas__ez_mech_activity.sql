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
    r.request_id,
    r.sender_address,
    r.prompt_link,
    d.delivery_link,
    {{ dbt_utils.generate_surrogate_key(
        ['r.mech_requests_id','d.mech_delivers_id']
    ) }} AS ez_mech_activity_id,
    GREATEST(
        COALESCE(
            r.inserted_timestamp,
            '1970-01-01' :: TIMESTAMP
        ),
        COALESCE(
            d.inserted_timestamp,
            '1970-01-01' :: TIMESTAMP
        )
    ) AS inserted_timestamp,
    GREATEST(
        COALESCE(
            r.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        ),
        COALESCE(
            d.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        )
    ) AS modified_timestamp
FROM
    {{ source(
        'gnosis_silver_olas',
        'mech_requests'
    ) }}
    r
    INNER JOIN {{ source(
        'gnosis_silver_olas',
        'mech_delivers'
    ) }}
    d USING(request_id)
