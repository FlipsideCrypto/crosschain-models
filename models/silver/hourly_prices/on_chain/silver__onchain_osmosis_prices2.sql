{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', id, recorded_hour)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE'],
    tags = ['prices']
) }}

WITH base AS (

    SELECT
        A.currency AS id,
        A.currency AS token_address,
        A.block_hour AS block_timestamp_hour,
        A.price_usd AS OPEN,
        A.price_usd AS high,
        A.price_usd AS low,
        A.price_usd AS CLOSE,
        'osmosis-swap' AS price_source,
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
        ) AS block_timestamp_hour,
        A.price_usd AS OPEN,
        A.price_usd AS high,
        A.price_usd AS low,
        A.price_usd AS CLOSE,
        'osmosis-pool-balance' AS price_source,
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
    DATEADD(
        HOUR,
        1,
        block_timestamp_hour
    ) AS recorded_hour,
    --roll the close price forward 1 hour
    OPEN,
    high,
    low,
    CLOSE,
    price_source,
    is_imputed,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['id','recorded_hour','price_source']) }} AS onchain_osmosis_prices_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base qualify(ROW_NUMBER() over (PARTITION BY id, recorded_hour, price_source
ORDER BY
    _inserted_timestamp DESC)) = 1
