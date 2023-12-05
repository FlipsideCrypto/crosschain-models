{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'merge',
    merge_update_columns = ['creator'],
) }}

WITH buyers AS (

    SELECT
        DISTINCT 'arbitrum' AS blockchain,
        'flipside' AS creator,
        buyer_address AS address,
        CONCAT(
            platform_name,
            ' user'
        ) AS tag_name,
        'nft' AS tag_type,
        MIN(
            block_timestamp :: DATE
        ) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        MIN(_INSERTED_TIMESTAMP) AS _INSERTED_TIMESTAMP
    FROM
        {{ source(
            'arbitrum_silver',
            'complete_nft_sales'
        ) }}

{% if is_incremental() %}
WHERE
    _INSERTED_TIMESTAMP > (
        SELECT
            MAX(_INSERTED_TIMESTAMP)
        FROM
            {{ this }}
    )
{% endif %}
GROUP BY
    blockchain,
    creator,
    buyer_address,
    tag_name,
    tag_type
),
sellers AS (
    SELECT
        DISTINCT 'arbitrum' AS blockchain,
        'flipside' AS creator,
        seller_address AS address,
        CONCAT(
            platform_name,
            ' user'
        ) AS tag_name,
        'nft' AS tag_type,
        MIN(
            block_timestamp :: DATE
        ) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        MIN(_INSERTED_TIMESTAMP) AS _INSERTED_TIMESTAMP
    FROM
        {{ source(
            'arbitrum_silver',
            'complete_nft_sales'
        ) }}

{% if is_incremental() %}
WHERE
    _INSERTED_TIMESTAMP > (
        SELECT
            MAX(_INSERTED_TIMESTAMP)
        FROM
            {{ this }}
    )
{% endif %}
GROUP BY
    blockchain,
    creator,
    seller_address,
    tag_name,
    tag_type
),
union_table AS (
    SELECT
        *
    FROM
        buyers
    UNION
    SELECT
        *
    FROM
        sellers
),
final_table AS (
    SELECT
        *
    FROM
        union_table qualify(ROW_NUMBER() over(PARTITION BY address
    ORDER BY
        start_date ASC)) = 1
)
SELECT
    A.*
FROM
    final_table A
