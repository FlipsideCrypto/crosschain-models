{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'merge',
    merge_update_columns = ['creator'],
) }}

WITH opensea_buyer AS (

    SELECT
        DISTINCT 'ethereum' AS blockchain,
        'flipside' AS creator,
        nft_to_address AS address,
        'opensea user' AS tag_name,
        'nft' AS tag_type,
        MIN(
            block_timestamp :: DATE
        ) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        MIN(_INSERTED_TIMESTAMP) AS ingested_at,
        'opensea' AS source
    FROM
        {{ source(
            'ethereum_silver_nft',
            'opensea_sales'
        ) }}

{% if is_incremental() %}
WHERE
    _INSERTED_TIMESTAMP > (
        SELECT
            MAX(ingested_at)
        FROM
            {{ this }}
        WHERE
            source = 'opensea'
    )
{% endif %}
GROUP BY
    nft_to_address
),
opensea_seller AS (
    SELECT
        DISTINCT 'ethereum' AS blockchain,
        'flipside' AS creator,
        nft_from_address AS address,
        'opensea user' AS tag_name,
        'nft' AS tag_type,
        MIN(
            block_timestamp :: DATE
        ) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        MIN(_INSERTED_TIMESTAMP) AS ingested_at,
        'opensea' AS source
    FROM
        {{ source(
            'ethereum_silver_nft',
            'opensea_sales'
        ) }}

{% if is_incremental() %}
WHERE
    _INSERTED_TIMESTAMP > (
        SELECT
            MAX(ingested_at)
        FROM
            {{ this }}
        WHERE
            source = 'opensea'
    )
{% endif %}
GROUP BY
    nft_from_address
),
seaport_buyer AS (
    SELECT
        DISTINCT 'ethereum' AS blockchain,
        'flipside' AS creator,
        buyer_address AS address,
        'opensea user' AS tag_name,
        'nft' AS tag_type,
        MIN(
            block_timestamp :: DATE
        ) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        MIN(ingested_at) AS ingested_at,
        'seaport' AS source
    FROM
        {{ source(
            'ethereum_silver_nft',
            'seaport_sales'
        ) }}

{% if is_incremental() %}
WHERE
    ingested_at > (
        SELECT
            MAX(ingested_at)
        FROM
            {{ this }}
        WHERE
            source = 'seaport'
    )
{% endif %}
GROUP BY
    buyer_address
),
seaport_seller AS (
    SELECT
        DISTINCT 'ethereum' AS blockchain,
        'flipside' AS creator,
        seller_address AS address,
        'opensea user' AS tag_name,
        'nft' AS tag_type,
        MIN(
            block_timestamp :: DATE
        ) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        MIN(ingested_at) AS ingested_at,
        'seaport' AS source
    FROM
        {{ source(
            'ethereum_silver_nft',
            'seaport_sales'
        ) }}

{% if is_incremental() %}
WHERE
    ingested_at > (
        SELECT
            MAX(ingested_at)
        FROM
            {{ this }}
        WHERE
            source = 'seaport'
    )
{% endif %}
GROUP BY
    seller_address
),
union_table AS (
    SELECT
        *
    FROM
        opensea_buyer
    UNION
    SELECT
        *
    FROM
        opensea_seller
    UNION
    SELECT
        *
    FROM
        seaport_buyer
    UNION
    SELECT
        *
    FROM
        seaport_seller
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
