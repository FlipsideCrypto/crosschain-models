{{ config(
    materialized = 'incremental',
    unique_key = ['blockchain', 'address'],
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['daily']
) }}

WITH ver AS (

    SELECT
        blockchain,
        address,
        LOWER(address) AS address_lower,
        MAX(is_verified) AS is_verified
    FROM
        {{ ref('silver__token_metadata') }}
    GROUP BY
        blockchain,
        address
),
cg_cmc AS (
    SELECT
        id,
        CASE
            WHEN blockchain = 'sei network' THEN 'sei'
            WHEN blockchain_name = 'osmosis' THEN 'osmosis'
            ELSE blockchain
        END AS blockchain,
        blockchain_name,
        token_address,
        LOWER(token_address) AS token_address_lower,
        source,
        symbol,
        provider,
        inserted_timestamp
    FROM
        {{ ref('silver__token_asset_metadata_all_providers') }}
),
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
        A.blockchain IN (
            'ton',
            'toncoin'
        )
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

{% if is_incremental() %}
WHERE
    modified_timestamp :: DATE >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    A.blockchain,
    A.address,
    COALESCE(
        b.is_verified,
        FALSE
    ) AS is_verified,
    A.symbol,
    A.decimals,
    A.name,
    COALESCE(
        cg.id,
        cg_add.id,
        cs1_cg.id,
        cs2_cg.id,
        c_ton_cg.id
    ) AS coingecko_id,
    COALESCE(
        cmc.id,
        cmc_add.id,
        cs1_cmc.id,
        cs2_cmc.id,
        c_ton_cmc.id
    ) AS coinmarketcap_id,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    token_base A
    LEFT JOIN ver b
    ON A.blockchain = b.blockchain
    AND A.address_lower = b.address_lower
    LEFT JOIN cg_cmc AS cg
    ON cg.provider = 'coingecko'
    AND A.blockchain = cg.blockchain
    AND A.address_lower = cg.token_address_lower
    LEFT JOIN cg_cmc AS cg_add
    ON cg_add.provider = 'coingecko'
    AND A.address_lower = cg_add.token_address_lower
    LEFT JOIN cg_cmc cmc
    ON cmc.provider = 'coinmarketcap'
    AND A.blockchain = cmc.blockchain
    AND A.address_lower = cmc.token_address_lower
    LEFT JOIN cg_cmc cmc_add
    ON cmc_add.provider = 'coinmarketcap'
    AND A.address_lower = cmc_add.token_address_lower
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
