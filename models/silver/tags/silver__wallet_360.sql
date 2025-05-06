{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address)",
    incremental_strategy = 'delete+insert'
) }}

{% if is_incremental() %}
WITH base AS (

    SELECT
        DISTINCT address,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN tag_name LIKE '%active on%' THEN blockchain
            END
        ) AS active_chains,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN tag_type = 'cex' THEN REPLACE(
                    tag_name,
                    ' user',
                    ''
                )
            END
        ) AS cex_used,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN tag_type = 'dex' THEN REPLACE(
                    tag_name,
                    ' user',
                    ''
                )
            END
        ) AS dex_used,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN tag_type = 'nft'
                AND tag_name LIKE '%user%' THEN REPLACE(
                    tag_name,
                    ' user',
                    ''
                )
            END
        ) AS nft_marketplace_used,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN tag_type = 'nft'
                AND tag_name NOT LIKE '%user%' THEN tag_name
            END
        ) AS nft_transactor,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN tag_type = 'wallet'
                AND end_date IS NULL THEN tag_name
            END
        ) AS wallet,
        MAX(tag_created_at) AS _inserted_timestamp
    FROM
        {{ ref('core__dim_tags') }}
    WHERE
        tag_name != 'contract address'
        AND tag_created_at >= (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        )
    GROUP BY
        address
),
full_tab AS (
    SELECT
        *
    FROM
        {{ this }}
    WHERE
        address IN (
            SELECT
                DISTINCT address
            FROM
                base
        )
    UNION
    SELECT
        *
    FROM
        base
)
SELECT
    DISTINCT address,
    array_union_agg(
        active_chains
    ) AS active_chains,
    array_union_agg(
        cex_used
    ) AS cex_used,
    array_union_agg(
        dex_used
    ) AS dex_used,
    array_union_agg(
        nft_marketplace_used
    ) AS nft_marketplace_used,
    array_union_agg(
        nft_transactor
    ) AS nft_transactor,
    array_union_agg(
        wallet
    ) AS wallet,
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    full_tab
GROUP BY
    address
{% else %}
SELECT
    DISTINCT address,
    ARRAY_AGG(
        DISTINCT CASE
            WHEN tag_name LIKE '%active on%' THEN blockchain
        END
    ) AS active_chains,
    ARRAY_AGG(
        DISTINCT CASE
            WHEN tag_type = 'cex' THEN REPLACE(
                tag_name,
                ' user',
                ''
            )
        END
    ) AS cex_used,
    ARRAY_AGG(
        DISTINCT CASE
            WHEN tag_type = 'dex' THEN REPLACE(
                tag_name,
                ' user',
                ''
            )
        END
    ) AS dex_used,
    ARRAY_AGG(
        DISTINCT CASE
            WHEN tag_type = 'nft'
            AND tag_name LIKE '%user%' THEN REPLACE(
                tag_name,
                ' user',
                ''
            )
        END
    ) AS nft_marketplace_used,
    ARRAY_AGG(
        DISTINCT CASE
            WHEN tag_type = 'nft'
            AND tag_name NOT LIKE '%user%' THEN tag_name
        END
    ) AS nft_transactor,
    ARRAY_AGG(
        DISTINCT CASE
            WHEN tag_type = 'wallet'
            AND end_date IS NULL THEN tag_name
        END
    ) AS wallet,
    MAX(tag_created_at) AS _inserted_timestamp
FROM
    {{ ref('core__dim_tags') }}
WHERE
    tag_name != 'contract address'
GROUP BY
    address
{% endif %}
