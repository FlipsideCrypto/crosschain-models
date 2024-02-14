{{ config(
    materialized = 'incremental',
    unique_key = ["address","blockchain"],
    incremental_strategy = 'merge',
    merge_update_columns = ['creator', 'modified_timestamp'],
) }}

WITH base AS (

    SELECT
        A.*,
        b.block_id
    FROM
        {{ source(
            'thorchain_defi',
            'fact_liquidity_actions'
        ) }} A
        JOIN {{ source(
            'thorchain_core',
            'dim_block'
        ) }}
        b
        ON A.dim_block_id = b.dim_block_id

{% if is_incremental() %}
WHERE
    DATE_TRUNC(
        'day',
        A.block_timestamp
    ) >= (
        SELECT
            MAX(start_date)
        FROM
            {{ this }}
    )
{% endif %}
),
lp_from AS (
    SELECT
        'thorchain' AS blockchain,
        'flipside' AS creator,
        from_address AS address,
        'thorchain liquidity provider' AS tag_name,
        'dex' AS tag_type,
        MIN(block_id) AS block_id,
        MIN(DATE_TRUNC('day', block_timestamp)) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        base
    WHERE
        lp_action = 'add_liquidity'
        AND address != 'NULL'
        AND address IS NOT NULL
    GROUP BY
        address
    ORDER BY
        address DESC
),
to_asset AS (
    SELECT
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
        MIN(block_id) AS block_id,
        MIN(DATE_TRUNC('day', block_timestamp)) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        base
    WHERE
        lp_action = 'add_liquidity'
        AND asset_address != 'NULL'
        AND asset_address IS NOT NULL
        AND blockchain != 'error'
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
    A.*,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address']) }} AS tags_thor_liquidity_provider_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    final_table A
