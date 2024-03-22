{{ config(
    materialized = 'incremental',
    unique_key = ['id','token_address','name','symbol','platform','platform_id','provider'],
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
        {{ ref('silver__all_asset_metadata_coingecko2') }}
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
        {{ ref('silver__all_asset_metadata_coinmarketcap2') }}
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
    id,
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
    {{ dbt_utils.generate_surrogate_key(['id','token_address','name','symbol','platform','platform_id','provider']) }} AS all_asset_metadata_all_providers_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_providers
