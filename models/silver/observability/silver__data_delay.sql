{{ config(
    materialized = 'incremental',
    unique_key = ['blockchain','test_timestamp']
) }}

WITH base AS (

    SELECT
        MAX(block_timestamp) AS max_timestamp,
        'arbitrum' AS chain,
        DATEDIFF(
            'minutes',
            max_timestamp,
            CURRENT_TIMESTAMP
        ) AS delay
    FROM
        {{ source(
            'arbitrum_silver',
            'traces'
        ) }}
    UNION
    SELECT
        MAX(block_timestamp) AS max_timestamp,
        'bsc' AS chain,
        DATEDIFF(
            'minutes',
            max_timestamp,
            CURRENT_TIMESTAMP
        ) AS delay
    FROM
        {{ source(
            'bsc_silver',
            'traces'
        ) }}
    UNION
    SELECT
        MAX(block_timestamp) AS max_timestamp,
        'ethereum' AS chain,
        DATEDIFF(
            'minutes',
            max_timestamp,
            CURRENT_TIMESTAMP
        ) AS delay
    FROM
        {{ source(
            'ethereum_silver',
            'traces'
        ) }}
    UNION
    SELECT
        MAX(block_timestamp) AS max_timestamp,
        'polygon' AS chain,
        DATEDIFF(
            'minutes',
            max_timestamp,
            CURRENT_TIMESTAMP
        ) AS delay
    FROM
        {{ source(
            'polygon_silver',
            'traces'
        ) }}
    UNION
    SELECT
        MAX(block_timestamp) AS max_timestamp,
        'base' AS chain,
        DATEDIFF(
            'minutes',
            max_timestamp,
            CURRENT_TIMESTAMP
        ) AS delay
    FROM
        {{ source(
            'base_silver',
            'traces'
        ) }}
    UNION
    SELECT
        MAX(block_timestamp) AS max_timestamp,
        'gnosis' AS chain,
        DATEDIFF(
            'minutes',
            max_timestamp,
            CURRENT_TIMESTAMP
        ) AS delay
    FROM
        {{ source(
            'gnosis_silver',
            'traces'
        ) }}
    UNION
    SELECT
        MAX(block_timestamp) AS max_timestamp,
        'avalanche' AS chain,
        DATEDIFF(
            'minutes',
            max_timestamp,
            CURRENT_TIMESTAMP
        ) AS delay
    FROM
        {{ source(
            'avalanche_silver',
            'traces'
        ) }}
    UNION
    SELECT
        MAX(block_timestamp) AS max_timestamp,
        'optimism' AS chain,
        DATEDIFF(
            'minutes',
            max_timestamp,
            CURRENT_TIMESTAMP
        ) AS delay
    FROM
        {{ source(
            'optimism_silver',
            'traces'
        ) }}
)
SELECT
    max_timestamp,
    chain AS blockchain,
    delay,
    'traces' AS test_table,
    CURRENT_TIMESTAMP AS test_timestamp
FROM
    base
