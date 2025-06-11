{{ config(
    materialized = 'incremental',
    unique_key = ['token_asset_metadata_enhanced_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['prices']
) }}

WITH

{% if is_incremental() %}
is_verified_modified AS (

    SELECT
        address,
        blockchain,
        coingecko_id,
        coinmarketcap_id,
        is_verified
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

cg_from_enhanced AS (
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
        A.decimals,
        A.blockchain,
        'coingecko' AS provider,
        'cg enhanced' AS source,
        FALSE AS is_deprecated,
        is_verified,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver__tokens_enhanced') }} A
        JOIN (
            SELECT
                id,
                NAME,
                symbol
            FROM
                {{ ref('silver__token_asset_metadata_coingecko') }}
                qualify ROW_NUMBER() over (
                    PARTITION BY id
                    ORDER BY
                        _inserted_timestamp DESC
                ) = 1
        ) b
        ON A.coingecko_id = b.id
    WHERE
        A.is_verified
        AND coingecko_id IS NOT NULL

{% if is_incremental() %}
AND (
    A.modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    OR A.blockchain || '--' || A.address IN (
        SELECT
            blockchain || '--' || address
        FROM
            is_verified_modified
    )
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
        A.decimals,
        A.blockchain,
        'coinmarketcap' AS provider,
        'cmc enhanced' AS source,
        FALSE AS is_deprecated,
        is_verified,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver__tokens_enhanced') }} A
        JOIN (
            SELECT
                id,
                token_address,
                NAME,
                symbol
            FROM
                {{ ref('silver__token_asset_metadata_coinmarketcap') }}
                qualify ROW_NUMBER() over (
                    PARTITION BY id
                    ORDER BY
                        _inserted_timestamp DESC
                ) = 1
        ) b
        ON A.coinmarketcap_id = b.id
    WHERE
        A.is_verified
        AND coinmarketcap_id IS NOT NULL

{% if is_incremental() %}
AND (
    A.modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    OR A.blockchain || '--' || A.address IN (
        SELECT
            blockchain || '--' || address
        FROM
            is_verified_modified
    )
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
    A.id,
    A.name,
    A.symbol,
    A.decimals,
    A.blockchain AS platform_adj,
    A.blockchain,
    A.blockchain AS blockchain_name,
    A.blockchain AS blockchain_id,
    A.provider,
    A.source,
    A.is_deprecated,
    A.is_verified,
    A._inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['LOWER(a.token_address)','a.blockchain','a.provider']) }} AS token_asset_metadata_enhanced_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_providers A
    LEFT JOIN {{ ref('silver__token_asset_metadata_all_providers') }}
    b
    ON A.token_address = b.token_address
    AND A.blockchain = b.blockchain
    AND A.provider = b.provider
WHERE
    b.token_address IS NULL qualify ROW_NUMBER() over (PARTITION BY LOWER(A.token_address), A.blockchain, A.provider
ORDER BY
    A._inserted_timestamp DESC) = 1
