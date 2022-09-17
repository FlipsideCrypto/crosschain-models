{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
) }}

WITH buyers AS (
    SELECT
        DISTINCT 'ethereum' AS blockchain,
        'flipside' AS creator,
        nft_to_address AS address,
        'looksrare user' AS tag_name,
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
            'looksrare_sales'
        ) }}

{% if is_incremental() %}
WHERE
    ingested_at > (select max(ingested_at) from {{this}})
{% endif %}
GROUP BY
    nft_to_address
),
sellers AS (
    SELECT
        DISTINCT 'ethereum' AS blockchain,
        'flipside' AS creator,
        nft_from_address AS address,
        'looksrare user' AS tag_name,
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
            'looksrare_sales'
        ) }}

{% if is_incremental() %}
WHERE
    ingested_at > (select max(ingested_at) from {{this}})
{% endif %}
GROUP BY
    nft_from_address
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