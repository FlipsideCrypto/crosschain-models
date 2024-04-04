{{ config(
    materialized = 'incremental',
    unique_key = ['complete_native_prices_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['hour::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(symbol, hour, blockchain)",
    tags = ['prices']
) }}

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
    ) AS symbol,
    m.name AS NAME,
    price,
    p.blockchain,
    is_imputed,
    m.is_deprecated,
    p.provider,
    p.source,
    p._inserted_timestamp,
    GREATEST(COALESCE(p.inserted_timestamp, '2000-01-01'), COALESCE(m.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(p.modified_timestamp, '2000-01-01'), COALESCE(m.modified_timestamp, '2000-01-01')) AS modified_timestamp,
    native_prices_priority_id AS complete_native_prices_id
FROM
    {{ ref('silver__native_prices_priority') }}
    p
    LEFT JOIN {{ ref('silver__complete_native_asset_metadata') }}
    m
    ON LOWER(
        p.symbol
    ) = LOWER(
        m.symbol
    )
    AND p.blockchain = m.blockchain

{% if is_incremental() %}
WHERE
    p._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY p.symbol, p.blockchain, HOUR
ORDER BY
    p._inserted_timestamp DESC)) = 1
