{{ config(
    materialized = 'incremental',
    unique_key = "unique_id",
    incremental_strategy = 'merge',
    merge_update_columns = ['creator', 'modified_timestamp'],
    tags = ['daily']
) }}

WITH from_cex AS (

    SELECT
        DISTINCT A.to_address AS address,
        b.project_name,
        MIN(
            A.block_timestamp :: DATE
        ) AS start_date,
        MIN(
            A.modified_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ source(
            'ethereum_core',
            'fact_transactions'
        ) }} A
        INNER JOIN (
            SELECT
                DISTINCT address,
                project_name
            FROM
                {{ source(
                    'ethereum_silver',
                    'labels'
                ) }}
            WHERE
                blockchain = 'ethereum'
                AND label_type = 'cex'
        ) b
        ON A.from_address = b.address
    WHERE
        A.to_address IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1,
    2
),
to_cex AS (
    SELECT
        DISTINCT A.from_address AS address,
        b.project_name,
        MIN(
            A.block_timestamp :: DATE
        ) AS start_date,
        MIN(
            A.modified_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ source(
            'ethereum_core',
            'fact_transactions'
        ) }} A
        INNER JOIN (
            SELECT
                DISTINCT address,
                project_name
            FROM
                {{ source(
                    'ethereum_silver',
                    'labels'
                ) }}
            WHERE
                blockchain = 'ethereum'
                AND label_type = 'cex'
        ) b
        ON A.to_address = b.address
    WHERE
        A.from_address IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1,
    2
),
total_table AS (
    SELECT
        DISTINCT 'ethereum' AS blockchain,
        'flipside' AS creator,
        address,
        CONCAT(
            project_name,
            ' user'
        ) AS tag_name,
        'cex' AS tag_type,
        start_date,
        NULL AS end_date,
        _inserted_timestamp,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        from_cex
    UNION
    SELECT
        DISTINCT 'ethereum' AS blockchain,
        'flipside' AS creator,
        address,
        CONCAT(
            project_name,
            ' user'
        ) AS tag_name,
        'cex' AS tag_type,
        start_date,
        NULL AS end_date,
        _inserted_timestamp,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        to_cex
),
total_table_small AS (
    SELECT
        *
    FROM
        total_table qualify(ROW_NUMBER() over(PARTITION BY address, tag_name
    ORDER BY
        start_date ASC)) = 1
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['address', 'tag_name']) }} AS unique_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','tag_name']) }} AS tags_cex_user_eth_id,
    '{{ invocation_id }}' as _invocation_id
FROM
    total_table_small
