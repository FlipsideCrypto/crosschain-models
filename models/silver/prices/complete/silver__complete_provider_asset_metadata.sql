{{ config(
    materialized = 'incremental',
    unique_key = 'complete_provider_asset_metadata_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['prices']
) }}

WITH coingecko AS (

    SELECT
        id,
        token_address,
        NAME,
        symbol,
        platform,
        platform_id,
        'coingecko' AS provider,
        source,
        _inserted_timestamp
    FROM
        {{ ref('bronze__all_asset_metadata_coingecko') }}

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
coinmarketcap AS (
    SELECT
        id,
        token_address,
        NAME,
        symbol,
        platform,
        platform_id,
        'coinmarketcap' AS provider,
        source,
        _inserted_timestamp
    FROM
        {{ ref('bronze__all_asset_metadata_coinmarketcap') }}

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
        coingecko
    UNION ALL
    SELECT
        *
    FROM
        coinmarketcap
)
SELECT
    id AS asset_id,
    token_address,
    NAME,
    symbol,
    platform,
    platform_id,
    provider,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['asset_id','token_address','name','symbol','platform','platform_id','provider']) }} AS complete_provider_asset_metadata_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_providers qualify(ROW_NUMBER() over (PARTITION BY asset_id, token_address, NAME, symbol, platform, platform_id, provider
ORDER BY
    _inserted_timestamp DESC)) = 1
