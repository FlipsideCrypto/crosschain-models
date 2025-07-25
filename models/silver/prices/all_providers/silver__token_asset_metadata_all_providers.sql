{{ config(
    materialized = 'incremental',
    unique_key = ['token_asset_metadata_all_providers_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['prices']
) }}

WITH coin_gecko AS (

    SELECT
        id,
        token_address,
        NAME,
        symbol,
        platform,
        platform_id,
        'coingecko' AS provider,
        source,
        is_deprecated,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__token_asset_metadata_coingecko'
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
        token_address,
        NAME,
        symbol,
        platform,
        platform_id,
        'coinmarketcap' AS provider,
        source,
        is_deprecated,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__token_asset_metadata_coinmarketcap'
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
ibc_am AS (
    SELECT
        address AS id,
        raw_metadata [0] :denom :: STRING AS token_address,
        CASE
            WHEN LENGTH(label) <= 0 THEN NULL
            ELSE label
        END AS NAME,
        CASE
            WHEN LENGTH(project_name) <= 0 THEN NULL
            ELSE project_name
        END AS symbol,
        'cosmos' AS platform,
        'cosmos' AS platform_id,
        'osmosis-onchain' AS provider,
        'ibc_am' AS source,
        FALSE AS is_deprecated,
        _inserted_timestamp
    FROM
        {{ source(
            'osmosis_silver',
            'asset_metadata'
        ) }}
    WHERE
        address IS NOT NULL
        AND LENGTH(address) > 0

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
UNION ALL
SELECT
    DISTINCT id,
    token_address,
    NULL AS NAME,
    NULL AS symbol,
    'cosmos' AS platform,
    'cosmos' AS platform_id,
    'osmosis-onchain' AS provider,
    'ibc_am' AS source,
    FALSE AS is_deprecated,
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    {{ ref('silver__onchain_osmosis_prices') }}
WHERE
    token_address IS NOT NULL
    AND LENGTH(token_address) > 0

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    ALL
),
solana_solscan AS (
    SELECT
        LOWER(
            CASE
                WHEN LENGTH(coingecko_id) <= 0
                OR coingecko_id IS NULL THEN token_address
                ELSE coingecko_id
            END
        ) AS id,
        token_address,
        CASE
            WHEN LENGTH(NAME) <= 0 THEN NULL
            ELSE NAME
        END AS NAME,
        CASE
            WHEN LENGTH(symbol) <= 0 THEN NULL
            ELSE symbol
        END AS symbol,
        'solana' AS platform,
        'solana' AS platform_id,
        'solscan' AS provider,
        'solscan' AS source,
        FALSE AS is_deprecated,
        _inserted_timestamp
    FROM
        {{ source(
            'solana_silver',
            'solscan_tokens'
        ) }}
    WHERE
        token_address IS NOT NULL
        AND LENGTH(token_address) > 0

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
solana_onchain AS (
    SELECT
        id,
        token_address,
        NAME,
        symbol,
        'solana' AS platform,
        'solana' AS platform_id,
        'solana-onchain' AS provider,
        'solana-onchain' AS source,
        FALSE AS is_deprecated,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver__onchain_solana_metadata') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
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
        ibc_am
    UNION ALL
    SELECT
        *
    FROM
        solana_solscan
    UNION ALL
    SELECT
        *
    FROM
        solana_onchain
)
SELECT
    token_address,
    id,
    A.name,
    A.symbol,
    b.platform_adj,
    COALESCE(
        b.blockchain,
        A.platform
    ) AS blockchain,
    A.platform AS blockchain_name,
    A.platform_id AS blockchain_id,
    A.provider,
    source,
    is_deprecated,
    COALESCE(
        C.is_verified,
        FALSE
    ) AS is_verified,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['LOWER(token_address)','a.platform_id','a.provider']) }} AS token_asset_metadata_all_providers_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_providers A
    LEFT JOIN {{ ref('silver__provider_platform_blockchain_map') }}
    b
    ON A.platform = b.platform
    AND A.provider = b.provider
    LEFT JOIN {{ ref('silver__tokens_enhanced') }} C
    ON A.token_address = C.address
    AND (LOWER(b.blockchain)) = LOWER(C.blockchain) qualify(ROW_NUMBER() over (PARTITION BY LOWER(token_address), A.platform_id, A.provider
ORDER BY
    _inserted_timestamp DESC)) = 1
