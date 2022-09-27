{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, tag_name)",
    incremental_strategy = 'merge',
    merge_update_columns = ['creator'],
) }}

WITH from_addresses AS (

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
        {{ source(
            'thorchain',
            'swaps'
        ) }}

{% if is_incremental() %}
WHERE
    block_id NOT IN (
        SELECT
            DISTINCT block_id
        FROM
            {{ this }}
    )
{% endif %}
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
        {{ source(
            'thorchain',
            'swaps'
        ) }}
    WHERE
        blockchain != 'error'
        AND native_to_address NOT IN (
            SELECT
                DISTINCT address
            FROM
                from_addresses
        )

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
),
final_table AS (
    SELECT
        *
    FROM
        from_addresses
    UNION
    SELECT
        *
    FROM
        to_addresses
)
SELECT
    A.*
FROM
    final_table A
