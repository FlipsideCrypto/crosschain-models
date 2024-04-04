{{ config(
    materialized = 'incremental',
    unique_key = ['native_asset_metadata_all_providers_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['prices']
) }}

WITH coin_gecko AS (

    SELECT
        id,
        NAME,
        symbol,
        'coingecko' AS provider,
        source,
        is_deprecated,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__native_asset_metadata_coingecko'
        ) }}

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
        id,
        NAME,
        symbol,
        'coinmarketcap' AS provider,
        source,
        is_deprecated,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__native_asset_metadata_coinmarketcap'
        ) }}

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
    id,
    NAME AS blockchain,
    symbol,
    provider,
    source,
    is_deprecated,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['symbol','provider']) }} AS native_asset_metadata_all_providers_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_providers p qualify(ROW_NUMBER() over (PARTITION BY symbol, provider
ORDER BY
    _inserted_timestamp DESC)) = 1
