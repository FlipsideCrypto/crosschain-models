{{ config(
    materialized = 'incremental',
    unique_key = ['blockchain', 'address'],
    merge_exclude_columns = ['inserted_timestamp'],
    full_refresh = false,
    tags = ['daily']
) }}

WITH -- get verified tokens based on transfer data
ver_xfer AS (

    SELECT
        blockchain,
        address,
        LOWER(address) AS address_lower,
        is_verified
    FROM
        {{ ref('silver__token_metadata') }}
    WHERE
        is_verified qualify ROW_NUMBER() over(
            PARTITION BY blockchain,
            address_lower
            ORDER BY
                address DESC
        ) = 1
),
-- get verified tokens from Uniswap labs
ver_external AS (
    SELECT
        DISTINCT CASE
            chain_id
            WHEN 42161 THEN 'arbiturm'
            WHEN 43114 THEN 'avalanche'
            WHEN 56 THEN 'bsc'
            WHEN 8453 THEN 'base'
            WHEN 81457 THEN 'blast'
            WHEN 1 THEN 'ethereum'
            WHEN 10 THEN 'optimism'
            WHEN 137 THEN 'polygon'
        END AS blockchain,
        LOWER(token_address) AS address_lower
    FROM
        {{ source(
            'external_tokenlists',
            'ez_verified_tokens'
        ) }}
    WHERE
        provider = 'Uniswap Labs Default'
        AND blockchain IS NOT NULL
),
cg AS (
    SELECT
        id,
        platform,
        token_address,
        LOWER(token_address) AS token_address_lower,
        source,
        symbol,
        'coingecko' AS provider,
        is_deprecated,
        inserted_timestamp
    FROM
        {{ ref('silver__token_asset_metadata_coingecko') }}
),
--Price provider metadata
cmc AS (
    SELECT
        id,
        platform,
        token_address,
        LOWER(token_address) AS token_address_lower,
        source,
        symbol,
        'coinmarketcap' AS provider,
        is_deprecated,
        inserted_timestamp
    FROM
        {{ ref('silver__token_asset_metadata_coinmarketcap') }}
),
cg_cmc AS (
    SELECT
        id,
        A.platform,
        b.blockchain,
        token_address,
        token_address_lower,
        source,
        symbol,
        A.provider,
        is_deprecated,
        inserted_timestamp
    FROM
        (
            SELECT
                id,
                platform,
                token_address,
                token_address_lower,
                source,
                symbol,
                provider,
                is_deprecated,
                inserted_timestamp
            FROM
                cg
            UNION ALL
            SELECT
                id,
                platform,
                token_address,
                token_address_lower,
                source,
                symbol,
                provider,
                is_deprecated,
                inserted_timestamp
            FROM
                cmc
        ) A
        LEFT JOIN {{ ref('silver__provider_platform_blockchain_map') }}
        b
        ON A.platform = b.platform
        AND A.provider = b.provider qualify ROW_NUMBER() over(
            PARTITION BY blockchain,
            token_address_lower,
            A.provider
            ORDER BY
                inserted_timestamp DESC
        ) = 1
),
cg_cmc_add_only AS (
    SELECT
        id,
        token_address_lower,
        provider
    FROM
        cg_cmc
    WHERE
        token_address_lower NOT IN (
            '0x7f27352d5f83db87a5a3e00f4b07cc2138d8ee52' -- real contract is bnb, on flow it's usdc
        ) qualify ROW_NUMBER() over(
            PARTITION BY token_address_lower
            ORDER BY
                is_deprecated,
                inserted_timestamp DESC
        ) = 1
),
cg_cmc_native AS (
    SELECT
        id,
        REPLACE(
            blockchain,
            ' protocol'
        ) AS blockchain,
        source,
        symbol,
        provider,
        inserted_timestamp
    FROM
        {{ ref('silver__native_asset_metadata_all_providers') }}
        qualify ROW_NUMBER() over(
            PARTITION BY blockchain,
            provider
            ORDER BY
                inserted_timestamp DESC
        ) = 1
),
--stellar uses weird token logic so we have a custom CTE to handle it
stell AS (
    SELECT
        UPPER(
            CASE
                WHEN len(TRIM(token_address)) = 56 THEN token_address
                ELSE SPLIT_PART(
                    token_address,
                    '-',
                    2
                )
            END
        ) AS asset_issuer,
        UPPER(
            CASE
                WHEN token_address LIKE '%-%' THEN SPLIT_PART(
                    token_address,
                    '-',
                    1
                )
                ELSE symbol
            END
        ) AS asset_code,*
    FROM
        cg_cmc
    WHERE
        blockchain = 'stellar' qualify ROW_NUMBER() over(
            PARTITION BY asset_issuer,
            asset_code,
            provider
            ORDER BY
                inserted_timestamp DESC
        ) = 1
),
-- the providers return the TON token address in the readable format we need to translate it to the raw to join back to our data
ton AS (
    SELECT
        A.blockchain,
        b.token_address_raw,
        LOWER(
            b.token_address_raw
        ) AS token_address_raw_lower,
        A.provider,
        A.id
    FROM
        cg_cmc A
        JOIN {{ ref('silver__tokens_ton_lookup') }}
        b USING(token_address)
    WHERE
        A.blockchain = 'ton'
),
-- Manual mapped tokens
man AS (
    SELECT
        blockchain,
        address,
        LOWER(address) AS address_lower,
        cg_id,
        cmc_id
    FROM
        {{ ref('silver__manual_verified_token_mapping') }}
    WHERE
        COALESCE(
            cg_id,
            cmc_id
        ) IS NOT NULL
        AND invalid_reason IS NULL --just double check that we don't have any dupes
        qualify ROW_NUMBER() over(
            PARTITION BY blockchain,
            address_lower
            ORDER BY
                address_lower DESC
        ) = 1
),
-- overrides for the is_verified flag
man_ver AS (
    SELECT
        blockchain,
        address,
        LOWER(address) AS address_lower,
        verified_override
    FROM
        {{ ref('silver__manual_verified_token_mapping') }}
    WHERE
        verified_override IS NOT NULL qualify ROW_NUMBER() over(
            PARTITION BY blockchain,
            address_lower
            ORDER BY
                address_lower DESC
        ) = 1
),
token_base AS (
    SELECT
        blockchain,
        address,
        LOWER(address) AS address_lower,
        symbol,
        decimals,
        NAME,
        SPLIT_PART(
            address,
            '-',
            1
        ) AS ton_asset_issuer,
        SPLIT_PART(
            address,
            '-',
            2
        ) AS ton_asset_code
    FROM
        {{ ref('silver__tokens') }}
        --Look for new tokens in the last 30 days to somewhat limit the lookback

{% if is_incremental() %}
WHERE
    inserted_timestamp :: DATE >= (
        SELECT
            MAX(modified_timestamp) :: DATE - 30
        FROM
            {{ this }}
    )
{% endif %}
),
FINAL AS (
    SELECT
        A.blockchain,
        A.address,
        CASE
            WHEN mv.verified_override IS NOT NULL THEN man_ver
            WHEN b.blockchain IS NOT NULL
            OR b_ex.blockchain IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_verified,
        CASE
            WHEN b_ex.blockchain IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_verified_external,
        A.symbol,
        A.decimals,
        A.name,
        COALESCE(
            man_cg.id,
            cg.id,
            cg_add.id,
            cs1_cg.id,
            cs2_cg.id,
            c_ton_cg.id,
            native_cg.id
        ) AS coingecko_id,
        COALESCE(
            man_cmc.id,
            cmc.id,
            cmc_add.id,
            cs1_cmc.id,
            cs2_cmc.id,
            c_ton_cmc.id,
            native_cmc.id
        ) AS coinmarketcap_id,
        CASE
            WHEN COALESCE(
                coingecko_id,
                coinmarketcap_id
            ) IS NOT NULL THEN TRUE
            ELSE FALSE
        END has_price_mapping
    FROM
        token_base A
        LEFT JOIN man_ver mv
        ON A.blockchain = man_ver.blockchain
        AND A.address_lower = man_ver.address_lower
        LEFT JOIN ver_xfer b
        ON A.blockchain = b.blockchain
        AND A.address_lower = b.address_lower
        LEFT JOIN ver_external b_ex
        ON A.blockchain = b_ex.blockchain
        AND A.address_lower = b_ex.address_lower
        LEFT JOIN cg_cmc AS cg
        ON cg.provider = 'coingecko'
        AND A.blockchain = cg.blockchain
        AND A.address_lower = cg.token_address_lower
        LEFT JOIN cg_cmc_add_only AS cg_add
        ON cg_add.provider = 'coingecko'
        AND A.address_lower = cg_add.token_address_lower
        AND cg.blockchain IS NULL
        LEFT JOIN cg_cmc cmc
        ON cmc.provider = 'coinmarketcap'
        AND A.blockchain = cmc.blockchain
        AND A.address_lower = cmc.token_address_lower
        LEFT JOIN cg_cmc_add_only cmc_add
        ON cmc_add.provider = 'coinmarketcap'
        AND A.address_lower = cmc_add.token_address_lower
        AND cmc.blockchain IS NULL
        LEFT JOIN stell cs1_cg
        ON A.blockchain = 'stellar'
        AND cs1_cg.provider = 'coingecko'
        AND A.ton_asset_issuer = cs1_cg.asset_issuer
        AND A.ton_asset_code = cs1_cg.asset_code
        LEFT JOIN stell cs2_cg
        ON A.blockchain = 'stellar'
        AND cs2_cg.provider = 'coinmarketcap'
        AND A.ton_asset_issuer = cs2_cg.asset_issuer
        AND A.ton_asset_code = cs2_cg.symbol
        LEFT JOIN stell cs1_cmc
        ON A.blockchain = 'stellar'
        AND cs1_cmc.provider = 'coinmarketcap'
        AND A.ton_asset_issuer = cs1_cmc.asset_issuer
        AND A.ton_asset_code = cs1_cmc.asset_code
        LEFT JOIN stell cs2_cmc
        ON A.blockchain = 'stellar'
        AND cs2_cmc.provider = 'coinmarketcap'
        AND A.ton_asset_issuer = cs2_cmc.asset_issuer
        AND A.ton_asset_code = cs2_cmc.symbol
        LEFT JOIN ton c_ton_cg
        ON A.blockchain = 'ton'
        AND c_ton_cg.provider = 'coingecko'
        AND A.address_lower = c_ton_cg.token_address_raw_lower
        LEFT JOIN ton c_ton_cmc
        ON A.blockchain = 'ton'
        AND c_ton_cmc.provider = 'coinmarketcap'
        AND A.address_lower = c_ton_cmc.token_address_raw_lower
        LEFT JOIN cg_cmc_native native_cg
        ON A.blockchain = native_cg.blockchain
        AND A.blockchain IN (
            'thorchain',
            'maya'
        )
        AND native_cg.provider = 'coingecko'
        LEFT JOIN cg_cmc_native native_cmc
        ON A.blockchain = native_cmc.blockchain
        AND A.blockchain IN (
            'thorchain',
            'maya'
        )
        AND native_cmc.provider = 'coinmarketcap'
        LEFT JOIN man man_cg
        ON A.blockchain = man_cg.blockchain
        AND A.address_lower = man_cg.address_lower
        AND A.id = man_cg.cg_id
        AND man_cg.cg_id IS NOT NULL
        LEFT JOIN man man_cmc
        ON A.blockchain = man_cmc.blockchain
        AND A.address_lower = man_cmc.address_lower
        AND A.id = man_cmc.cmc_id
        AND man_cmc.cmc_id IS NOT NULL
)
SELECT
    A.blockchain,
    A.address,
    A.is_verified,
    A.is_verified_external,
    A.symbol,
    A.decimals,
    A.name,
    A.coingecko_id,
    A.coinmarketcap_id,
    A.has_price_mapping,

{% if is_incremental() %}
CASE
    WHEN A.is_verified THEN COALESCE(curr_val.first_verified_timestamp, SYSDATE())END AS first_verified_timestamp,
    {% else %}
        CASE
            WHEN A.is_verified THEN SYSDATE()
        END AS first_verified_timestamp,
    {% endif %}

    {{ dbt_utils.generate_surrogate_key(['a.address','a.blockchain' ]) }} AS tokens_enhanced_id,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
    FROM
        FINAL A

{% if is_incremental() %}
LEFT JOIN {{ this }}
curr_val
ON A.address = curr_val.address
AND A.blockchain = curr_val.blockchain
{% endif %}
