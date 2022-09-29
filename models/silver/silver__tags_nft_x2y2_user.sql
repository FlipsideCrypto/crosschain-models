{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'merge',
    merge_update_columns = ['creator'],
) }}

WITH buyers AS (

    SELECT
        DISTINCT 'ethereum' AS blockchain,
        'flipside' AS creator,
        buyer_address AS address,
        'x2y2 user' AS tag_name,
        'nft' AS tag_type,
        MIN(
            block_timestamp :: DATE
        ) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        MIN(ingested_at) AS ingested_at
    FROM
        {{ source(
            'ethereum_silver_nft',
            'x2y2_sales'
        ) }}

{% if is_incremental() %}
WHERE
    ingested_at > (
        SELECT
            MAX(ingested_at)
        FROM
            {{ this }}
    )
{% endif %}
GROUP BY
    buyer_address
),
sellers AS (
    SELECT
        DISTINCT 'ethereum' AS blockchain,
        'flipside' AS creator,
        seller_address AS address,
        'x2y2 user' AS tag_name,
        'nft' AS tag_type,
        MIN(
            block_timestamp :: DATE
        ) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        MIN(ingested_at) AS ingested_at
    FROM
        {{ source(
            'ethereum_silver_nft',
            'x2y2_sales'
        ) }}

{% if is_incremental() %}
WHERE
    ingested_at > (
        SELECT
            MAX(ingested_at)
        FROM
            {{ this }}
    )
{% endif %}
GROUP BY
    seller_address
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
