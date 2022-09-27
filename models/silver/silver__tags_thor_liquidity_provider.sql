{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, tag_name)",
    incremental_strategy = 'merge',
    merge_update_columns = ['creator'],
) }}

WITH lp_from AS (

    SELECT
        DISTINCT 'thorchain' AS blockchain,
        'flipside' AS creator,
        from_address AS address,
        'thorchain liquidity provider' AS tag_name,
        'dex' AS tag_type,
        MIN(block_id) AS block_id,
        MIN(DATE_TRUNC('day', block_timestamp)) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        {{ source(
            'thorchain',
            'liquidity_actions'
        ) }}
    WHERE
        lp_action = 'add_liquidity'
        AND address != 'NULL'
        AND address IS NOT NULL

{% if is_incremental() %}
AND block_id NOT IN (
    SELECT
        DISTINCT block_id
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    address
ORDER BY
    address DESC
),
to_asset AS (
    SELECT
        DISTINCT CASE
            WHEN LEFT(
                asset_address,
                3
            ) = 'bnb' THEN 'bsc'
            WHEN LEFT(
                asset_address,
                6
            ) = 'cosmos' THEN 'cosmos'
            WHEN LEFT(
                asset_address,
                2
            ) = '0x' THEN 'ethereum'
            WHEN LEFT(
                asset_address,
                5
            ) = 'terra' THEN 'terra'
            WHEN LEFT(
                asset_address,
                1
            ) = 'q' THEN 'bitcoin cash'
            WHEN LEFT(
                asset_address,
                4
            ) = 'thor' THEN 'thorchain'
            WHEN LEFT(
                asset_address,
                3
            ) = 'ltc' THEN 'litecoin'
            WHEN LEFT(
                asset_address,
                1
            ) = 'M' THEN 'litecoin'
            WHEN LEFT(
                asset_address,
                1
            ) = 'L' THEN 'litecoin'
            WHEN LEFT(
                asset_address,
                1
            ) = 'D' THEN 'dogechain'
            WHEN LEFT(
                asset_address,
                2
            ) = 'bc' THEN 'bitcoin'
            WHEN LEFT(
                asset_address,
                1
            ) = '1' THEN 'bitcoin'
            WHEN LEFT(
                asset_address,
                1
            ) = '3' THEN 'bitcoin'
            ELSE 'error'
        END AS blockchain,
        'flipside' AS creator,
        asset_address AS address,
        'thorchain liquidity provider' AS tag_name,
        'dex' AS tag_type,
        MIN(block_id) AS block_id,
        MIN(DATE_TRUNC('day', block_timestamp)) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        {{ source(
            'thorchain',
            'liquidity_actions'
        ) }}
    WHERE
        lp_action = 'add_liquidity'
        AND asset_address != 'NULL'
        AND asset_address IS NOT NULL
        AND blockchain != 'error'

{% if is_incremental() %}
AND block_id NOT IN (
    SELECT
        DISTINCT block_id
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    address
ORDER BY
    address DESC
),
final_table AS (
    SELECT
        *
    FROM
        lp_from
    UNION
    SELECT
        *
    FROM
        to_asset
)
SELECT
    A.*
FROM
    final_table A
