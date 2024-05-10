{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'merge',
    merge_update_columns = ['creator'],
    tags = ['daily']
) }}

WITH traders AS (

    SELECT
        DISTINCT 'base' AS blockchain,
        'flipside' AS creator,
        origin_from_address AS address,
        CONCAT(
            platform,
            ' user'
        ) AS tag_name,
        'dex' AS tag_type,
        MIN(
            block_timestamp :: DATE
        ) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        MIN(_INSERTED_TIMESTAMP) AS _INSERTED_TIMESTAMP
    FROM
        {{ source(
            'base_silver_dex',
            'complete_dex_swaps'
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
    origin_from_address,
    tag_name,
    tag_type
),
final_table AS (
    SELECT
        *
    FROM
        traders qualify(ROW_NUMBER() over(PARTITION BY address
    ORDER BY
        start_date ASC)) = 1
)
SELECT
    A.*
FROM
    final_table A
