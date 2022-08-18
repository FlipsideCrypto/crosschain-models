{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
) }}

with lp_from as (
SELECT
    DISTINCT 'thorchain' AS blockchain,
    'flipside' AS creator,
    from_address AS address,
    'thorchain liquidity provider' AS tag_name,
    'dex' AS tag_type,
    MIN(block_id) as block_id,
    MIN(DATE_TRUNC('day', block_timestamp)) AS start_date,
    NULL AS end_date,
    current_timestamp as tag_created_at
FROM
    {{source('thorchain', 'liquidity_actions')}}
WHERE
    lp_action = 'add_liquidity'
    AND address != 'NULL'
    AND address IS NOT NULL

    {% if is_incremental() %}
    and
        block_id not in (
            SELECT
                distinct block_id
            FROM
                {{ this }}
        )
    {% endif %}

GROUP BY
    address
ORDER BY
    address DESC
),
to_asset as (
    SELECT
    DISTINCT 
    CASE
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
    MIN(block_id) as block_id,
    MIN(DATE_TRUNC('day', block_timestamp)) AS start_date,
    NULL AS end_date,
    current_timestamp as tag_created_at
FROM
    {{source('thorchain', 'liquidity_actions')}}
WHERE
    lp_action = 'add_liquidity'
    AND asset_address != 'NULL'
    AND asset_address IS NOT NULL
    AND blockchain != 'error'

    {% if is_incremental() %}
    and
        block_id not in (
            SELECT
                distinct block_id
            FROM
                {{ this }}
        )
    {% endif %}

GROUP BY
    address
ORDER BY
    address DESC
),
final_table as (
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
select a.* from final_table a
{% if is_incremental() %}
left outer join {{this}} b on final_table.address = b.address 
{% endif %}
