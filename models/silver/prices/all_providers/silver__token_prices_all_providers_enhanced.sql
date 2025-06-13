{{ config(
    materialized = 'incremental',
    unique_key = ['token_prices_all_providers_enhanced_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE'],
    tags = ['prices']
) }}
-- depends_on: {{ ref('silver__tokens_enhanced') }}
WITH

{% if is_incremental() %}
is_verified_modified AS (

    SELECT
        address,
        blockchain,
        coingecko_id,
        coinmarketcap_id
    FROM
        {{ ref('silver__tokens_enhanced') }}
    WHERE
        is_verified_modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
),
{% endif %}

coin_gecko AS (
    SELECT
        recorded_hour,
        CLOSE AS price,
        is_imputed,
        id,
        'coingecko' AS provider,
        source,
        _inserted_timestamp
    FROM
        {{ ref('silver__prices_coingecko') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    OR id IN (
        SELECT
            coingecko_id
        FROM
            is_verified_modified
        WHERE
            coingecko_id IS NOT NULL
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
        {{ ref('silver__prices_coinmarketcap') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    OR id IN (
        SELECT
            coinmarketcap_id
        FROM
            is_verified_modified
        WHERE
            coinmarketcap_id IS NOT NULL
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
        A._inserted_timestamp,
        CASE
            WHEN A.provider = 'coingecko' THEN 1
            WHEN A.provider = 'coinmarketcap' THEN 2
            ELSE 3
        END AS provider_order
    FROM
        all_providers A
        JOIN {{ ref('silver__token_asset_metadata_enhanced') }}
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
    {{ dbt_utils.generate_surrogate_key(['recorded_hour','LOWER(token_address)','blockchain_id']) }} AS token_prices_all_providers_enhanced_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    mapping qualify(ROW_NUMBER() over (PARTITION BY recorded_hour, LOWER(token_address), blockchain_id
ORDER BY
    provider_order, _inserted_timestamp DESC)) = 1
