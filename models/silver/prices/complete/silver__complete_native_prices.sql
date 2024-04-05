{{ config(
    materialized = 'incremental',
    unique_key = ['complete_native_prices_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['hour::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(symbol, hour, blockchain)",
    tags = ['prices']
) }}

WITH base_prices AS (

    SELECT
        DATEADD(
            HOUR,
            1,
            recorded_hour
        ) AS HOUR,
        --roll the close price forward 1 hour
        p.id AS asset_id,
        UPPER(
            p.symbol
        ) AS symbol_adj,
        m.name,
        m.decimals,
        price,
        p.blockchain,
        is_imputed,
        m.is_deprecated,
        p.provider,
        p.source,
        p._inserted_timestamp
    FROM
        {{ ref('silver__native_prices_priority') }}
        p
        LEFT JOIN {{ ref('silver__complete_native_asset_metadata') }}
        m
        ON UPPER(
            p.symbol
        ) = m.symbol

{% if is_incremental() %}
WHERE
    p._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    OR symbol_adj NOT IN (
        SELECT
            symbol
        FROM
            (
                SELECT
                    DISTINCT symbol
                FROM
                    {{ this }}
            )
    ) --load all data for new assets, requires additional select statement for compiler
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY symbol_adj, HOUR
ORDER BY
    p._inserted_timestamp DESC)) = 1
)
SELECT
    HOUR,
    asset_id,
    symbol_adj AS symbol,
    NAME,
    decimals,
    price,
    blockchain,
    is_imputed,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['HOUR','symbol']) }} AS complete_native_prices_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_prices
