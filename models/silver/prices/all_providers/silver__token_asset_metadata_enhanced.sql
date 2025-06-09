{{ config(
    materialized = 'incremental',
    unique_key = ['token_asset_metadata_enhanced_id'],
    incremental_strategy = 'delete+insert',
    tags = ['prices']
) }}

WITH cg_from_enhanced AS (

    SELECT
        A.coingecko_id AS id,
        A.address AS token_address,
        COALESCE(
            A.name,
            b.name
        ) AS NAME,
        COALESCE(
            A.symbol,
            b.symbol
        ) AS symbol,
        A.decimals decimals,
        blockchain AS platform,
        blockchain AS platform_id,
        'coingecko' AS provider,
        'cg enhanced' AS source,
        FALSE AS is_deprecated,
        is_verified,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver__tokens_enhanced') }} A
        LEFT JOIN (
            SELECT
                id,
                token_address,
                NAME,
                symbol
            FROM
                {{ ref('silver__token_asset_metadata_coingecko') }}
                qualify ROW_NUMBER() over (
                    PARTITION BY id,
                    token_address
                    ORDER BY
                        _inserted_timestamp DESC
                ) = 1
        ) b
        ON A.coingecko_id = b.id
    WHERE
        A.is_verified
        AND LOWER(
            A.address
        ) <> LOWER(b.token_address)

{% if is_incremental() %}
AND A.modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
cmc_from_enhanced AS (
    SELECT
        A.coinmarketcap_id AS id,
        A.address AS token_address,
        COALESCE(
            A.name,
            b.name
        ) AS NAME,
        COALESCE(
            A.symbol,
            b.symbol
        ) AS symbol,
        A.decimals decimals,
        blockchain AS platform,
        blockchain AS platform_id,
        'coinmarketcap' AS provider,
        'cmc enhanced' AS source,
        FALSE AS is_deprecated,
        is_verified,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver__tokens_enhanced') }} A
        LEFT JOIN (
            SELECT
                id,
                token_address,
                NAME,
                symbol
            FROM
                {{ ref('silver__token_asset_metadata_coinmarketcap') }}
                qualify ROW_NUMBER() over (
                    PARTITION BY id,
                    token_address
                    ORDER BY
                        _inserted_timestamp DESC
                ) = 1
        ) b
        ON A.coinmarketcap_id = b.id
    WHERE
        A.is_verified
        AND blockchain NOT IN ('osmosis')
        AND LOWER(
            A.address
        ) <> LOWER(b.token_address)

{% if is_incremental() %}
AND A.modified_timestamp >= (
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
        cg_from_enhanced
    UNION ALL
    SELECT
        *
    FROM
        cmc_from_enhanced
)
SELECT
    A.token_address,
    id,
    A.name,
    A.symbol,
    A.decimals,
    b.platform_adj,
    b.blockchain,
    A.platform AS blockchain_name,
    A.platform_id AS blockchain_id,
    A.provider,
    source,
    is_deprecated,
    is_verified,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['LOWER(a.token_address)','b.blockchain','a.provider']) }} AS token_asset_metadata_enhanced_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_providers A
    LEFT JOIN {{ ref('silver__provider_platform_blockchain_map') }}
    b
    ON A.platform = b.platform
    AND A.provider = b.provider qualify ROW_NUMBER() over (PARTITION BY LOWER(A.token_address), b.blockchain, A.provider
ORDER BY
    _inserted_timestamp DESC) = 1
