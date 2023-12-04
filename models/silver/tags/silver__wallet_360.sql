{{ config(
    materialized = 'table',
    unique_key = "CONCAT_WS('-', address, blockchain)"
) }}

WITH base AS (

    SELECT
        DISTINCT blockchain,
        address,
        tag_type,
        CASE
            WHEN tag_name LIKE '%active on%' THEN CONCAT(
                'active on ',
                blockchain
            )
            ELSE tag_name
        END AS tag_name
    FROM
        {{ ref('core__dim_tags') }}
    WHERE
        creator = 'flipside'
        AND tag_type IN (
            'contract',
            'token standard',
            'activity',
            'cex',
            'wallet',
            'nft',
            'dex'
        )
)
SELECT
    DISTINCT p.*
FROM
    base pivot(ARRAY_AGG(tag_name) FOR tag_type IN ('contract', 'token standard', 'activity', 'cex', 'wallet', 'nft', 'dex')) AS p
