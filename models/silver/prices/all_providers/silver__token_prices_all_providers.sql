{{ config(
    materialized = 'incremental',
    unique_key = ['token_prices_all_providers_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE'],
    tags = ['prices']
) }}

WITH coin_gecko AS (

    SELECT
        recorded_hour,
        CLOSE AS price,
        is_imputed,
        id,
        'coingecko' AS provider,
        source,
        _inserted_timestamp
    FROM
        {{ ref('silver__token_prices_coingecko') }}

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
        CLOSE AS price,
        is_imputed,
        id,
        'coinmarketcap' AS provider,
        source,
        _inserted_timestamp
    FROM
        {{ ref('silver__token_prices_coinmarketcap') }}

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
ibc_prices AS (
    SELECT
        recorded_hour,
        CLOSE AS price,
        is_imputed,
        id,
        price_source AS provider,
        'ibc_prices' AS source,
        _inserted_timestamp
    FROM
        {{ ref('silver__onchain_osmosis_prices') }}

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
    UNION ALL
    SELECT
        *
    FROM
        ibc_prices
),
mapping AS (
    SELECT
        A.recorded_hour,
        A.id,
        A.price,
        A.is_imputed,
        b.token_address,
        b.platform_adj,
        b.blockchain,
        b.blockchain_name,
        b.blockchain_id,
        A.provider,
        A.source,
        A._inserted_timestamp
    FROM
        all_providers A
        JOIN {{ ref('silver__token_asset_metadata_all_providers') }}
        b
        ON A.id = b.id
)
SELECT
    recorded_hour,
    token_address,
    blockchain,
    blockchain_name,
    blockchain_id,
    price,
    is_imputed,
    id,
    provider,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['recorded_hour','LOWER(token_address)','blockchain_id','provider']) }} AS token_prices_all_providers_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    mapping qualify(ROW_NUMBER() over (PARTITION BY recorded_hour, LOWER(token_address), blockchain_id, provider
ORDER BY
    _inserted_timestamp DESC)) = 1
