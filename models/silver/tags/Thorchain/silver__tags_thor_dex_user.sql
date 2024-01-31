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
            'fact_swaps'
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
from_addresses AS (
    SELECT
        DISTINCT CASE
            blockchain
            WHEN 'BNB' THEN 'bsc'
            WHEN 'GAIA' THEN 'cosmos'
            WHEN 'ETH' THEN 'ethereum'
            WHEN 'TERRA' THEN 'terra'
            WHEN 'BCH' THEN 'bitcoin cash'
            WHEN 'THOR' THEN 'thorchain'
            WHEN 'LTC' THEN 'litecoin'
            WHEN 'DOGE' THEN 'dogechain'
            WHEN 'BTC' THEN 'bitcoin'
            ELSE 'error'
        END AS blockchain,
        'flipside' AS creator,
        from_address AS address,
        'thorchain dex user' AS tag_name,
        'dex' AS tag_type,
        MIN(block_id) AS block_id,
        MIN(DATE_TRUNC('day', block_timestamp)) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        base
    WHERE
        (
            blockchain NOT IN (
                'error',
                'NULL',
                'null',
                'Null'
            )
            OR blockchain IS NOT NULL
        )
    GROUP BY
        blockchain,
        address
),
to_addresses AS (
    SELECT
        DISTINCT CASE
            WHEN LEFT(
                native_to_address,
                3
            ) = 'bnb' THEN 'bsc'
            WHEN LEFT(
                native_to_address,
                6
            ) = 'cosmos' THEN 'cosmos'
            WHEN LEFT(
                native_to_address,
                2
            ) = '0x' THEN 'ethereum'
            WHEN LEFT(
                native_to_address,
                5
            ) = 'terra' THEN 'terra'
            WHEN LEFT(
                native_to_address,
                1
            ) = 'q' THEN 'bitcoin cash'
            WHEN LEFT(
                native_to_address,
                4
            ) = 'thor' THEN 'thorchain'
            WHEN LEFT(
                native_to_address,
                3
            ) = 'ltc' THEN 'litecoin'
            WHEN LEFT(
                native_to_address,
                1
            ) = 'M' THEN 'litecoin'
            WHEN LEFT(
                native_to_address,
                1
            ) = 'L' THEN 'litecoin'
            WHEN LEFT(
                native_to_address,
                1
            ) = 'D' THEN 'dogechain'
            WHEN LEFT(
                native_to_address,
                2
            ) = 'bc' THEN 'bitcoin'
            WHEN LEFT(
                native_to_address,
                1
            ) = '1' THEN 'bitcoin'
            WHEN LEFT(
                native_to_address,
                1
            ) = '3' THEN 'bitcoin'
            ELSE 'error'
        END AS blockchain,
        'flipside' AS creator,
        native_to_address AS address,
        'thorchain dex user' AS tag_name,
        'dex' AS tag_type,
        MIN(block_id) AS block_id,
        MIN(DATE_TRUNC('day', block_timestamp)) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        base
    WHERE
        blockchain != 'error'
        AND native_to_address NOT IN (
            SELECT
                DISTINCT address
            FROM
                from_addresses
        )
    GROUP BY
        address
),
final_table AS (
    SELECT
        *
    FROM
        from_addresses
    WHERE
        blockchain != 'error'
    UNION
    SELECT
        *
    FROM
        to_addresses
    WHERE
        blockchain != 'error'
)
SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address']) }} AS tags_thor_dex_user_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    final_table
