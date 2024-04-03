{{ config(
    materialized = 'incremental',
    unique_key = ['native_prices_all_providers_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE'],
    tags = ['prices']
) }}

WITH coin_gecko AS (

    SELECT
        recorded_hour,
        symbol,
        id,
        platform,
        CLOSE AS price,
        is_imputed,
        'coingecko' AS provider,
        source,
        _inserted_timestamp
    FROM
        {{ ref('silver__native_prices_coingecko') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
coin_market_cap AS (
    SELECT
        recorded_hour,
        symbol,
        id,
        platform,
        CLOSE AS price,
        is_imputed,
        'coinmarketcap' AS provider,
        source,
        _inserted_timestamp
    FROM
        {{ ref('silver__native_prices_coinmarketcap') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
all_providers AS (
    SELECT
        *
    FROM
        coin_gecko
    UNION ALL
    SELECT
        *
    FROM
        coin_market_cap
)
SELECT
    recorded_hour,
    symbol,
    id,
    platform AS blockchain,
    price,
    is_imputed,
    provider,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['recorded_hour','symbol','blockchain','provider']) }} AS native_prices_all_providers_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_providers qualify(ROW_NUMBER() over (PARTITION BY recorded_hour, symbol, blockchain, provider
ORDER BY
    _inserted_timestamp DESC)) = 1
