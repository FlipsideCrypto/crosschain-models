{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'merge',
    merge_update_columns = ['creator', 'modified_timestamp'],
) }}

WITH buyers AS (

    SELECT
        DISTINCT 'ethereum' AS blockchain,
        'flipside' AS creator,
        buyer_address AS address,
        'nftx user' AS tag_name,
        'nft' AS tag_type,
        MIN(
            block_timestamp :: DATE
        ) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        MIN(_INSERTED_TIMESTAMP) AS _INSERTED_TIMESTAMP
    FROM
        {{ source(
            'ethereum_silver_nft',
            'nftx_sales'
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
    buyer_address
),
sellers AS (
    SELECT
        DISTINCT 'ethereum' AS blockchain,
        'flipside' AS creator,
        seller_address AS address,
        'nftx user' AS tag_name,
        'nft' AS tag_type,
        MIN(
            block_timestamp :: DATE
        ) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        MIN(_INSERTED_TIMESTAMP) AS _INSERTED_TIMESTAMP
    FROM
        {{ source(
            'ethereum_silver_nft',
            'nftx_sales'
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
    A.*,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address']) }} AS tags_nft_nftx_user_id,
    '{{ invocation_id }}' as _invocation_id  
FROM
    final_table A
