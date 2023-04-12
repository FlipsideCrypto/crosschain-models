{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', id, recorded_hour)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE'],
) }}

WITH base AS (

    SELECT
        A.currency AS id,
        A.currency AS token_address,
        A.block_hour AS recorded_hour,
        A.price_usd AS OPEN,
        A.price_usd AS high,
        A.price_usd AS low,
        A.price_usd AS CLOSE,
        'swaps' AS source,
        CASE
            WHEN swaps_in_hour IS NULL THEN TRUE
            ELSE FALSE
        END AS is_imputed,
        A.block_hour AS _inserted_timestamp
    FROM
        {{ source(
            'osmosis_silver',
            'prices_swaps'
        ) }} A
    UNION ALL
    SELECT
        A.token_address AS id,
        A.token_address AS token_address,
        DATE_TRUNC(
            'hour',
            A.block_timestamp
        ) AS recorded_hour,
        A.price_usd AS OPEN,
        A.price_usd AS high,
        A.price_usd AS low,
        A.price_usd AS CLOSE,
        'pool balance' AS source,
        FALSE AS is_imputed,
        A._inserted_timestamp
    FROM
        {{ source(
            'osmosis_silver',
            'pool_token_prices_usd'
        ) }} A
)
SELECT
    id,
    token_address,
    recorded_hour,
    OPEN,
    high,
    low,
    CLOSE,
    source,
    is_imputed,
    _inserted_timestamp
FROM
    base qualify(ROW_NUMBER() over (PARTITION BY id, recorded_hour, source
ORDER BY
    _inserted_timestamp DESC)) = 1
