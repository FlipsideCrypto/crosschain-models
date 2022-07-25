{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
) }}

SELECT
    DISTINCT 'thorchain' AS blockchain,
    'flipside' AS creator,
    from_address AS address,
    'thorchain liquidity provider' AS tag_name,
    'dex' AS tag_type,
    MIN(DATE_TRUNC('day', block_timestamp)) AS start_date,
    NULL AS end_date
FROM
    {{source('thorchain', 'liquidity_actions')}}
WHERE
    lp_action = 'add_liquidity'
    AND address != 'NULL'
    AND address IS NOT NULL
    {% if is_incremental() %}
    AND
    block_timestamp > (
        SELECT
            MAX(start_date)
        FROM
            {{ this }}
    )
    {% endif %}
GROUP BY
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    end_date
ORDER BY
    address DESC
