{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge'
) }}

WITH coin_gecko_meta AS (

    SELECT
        DISTINCT CASE
            WHEN TRIM(token_address) ILIKE '^x%'
            OR TRIM(token_address) ILIKE '0x%' THEN REGEXP_SUBSTR(REGEXP_REPLACE(token_address, '^x', '0x'), '0x[a-zA-Z0-9]*')
            WHEN id = 'osmosis' THEN 'uosmo'
            WHEN id = 'algorand' THEN '0'
            ELSE token_address
        END AS token_address,
        LOWER(id) AS id,
        LOWER(symbol) AS symbol,
        LOWER(
            CASE
                WHEN id = 'osmosis' THEN 'osmosis'
                WHEN id = 'algorand' THEN 'algorand'
                ELSE platform :: STRING
            END
        ) AS platform,
        'coingecko' AS provider,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__asset_metadata_coin_gecko'
        ) }}
),
coin_market_cap_meta AS (
    SELECT
        DISTINCT CASE
            WHEN TRIM(token_address) ILIKE '^x%'
            OR TRIM(token_address) ILIKE '0x%' THEN REGEXP_SUBSTR(REGEXP_REPLACE(token_address, '^x', '0x'), '0x[a-zA-Z0-9]*')
            WHEN id = '12220' THEN 'uosmo'
            WHEN id = '4030' THEN '0'
            ELSE token_address
        END AS token_address,
        LOWER(id) AS id,
        LOWER(symbol) AS symbol,
        LOWER(
            CASE
                WHEN id = '12220' THEN 'osmosis'
                WHEN id = '4030' THEN 'Algorand'
                ELSE platform :: STRING
            END
        ) AS platform,
        'coinmarketcap' AS provider,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__asset_metadata_coin_market_cap'
        ) }}
),
legacy_coin_gecko_meta AS (
    SELECT
        m.token_address AS token_address,
        LOWER(
            p.asset_id
        ) AS id,
        LOWER(COALESCE(p.symbol, m.symbol)) AS symbol,
        LOWER(
            p.platform :name :: STRING
        ) AS platform,
        p.provider,
        _inserted_timestamp
    FROM
        {{ source(
            'legacy_db',
            'prices_v2'
        ) }}
        p
        LEFT JOIN coin_gecko_meta m
        ON m.id = p.asset_id
    WHERE
        p.provider = 'coingecko'
        AND recorded_at :: DATE < '2022-08-24'
        AND m.token_address IS NOT NULL
        AND p.platform IS NOT NULL
),
legacy_coin_market_cap_meta AS (
    SELECT
        m.token_address AS token_address,
        LOWER(
            p.asset_id
        ) AS id,
        LOWER(COALESCE(p.symbol, m.symbol)) AS symbol,
        LOWER(
            p.platform :name :: STRING
        ) AS platform,
        p.provider,
        _inserted_timestamp
    FROM
        {{ source(
            'legacy_db',
            'prices_v2'
        ) }}
        p
        LEFT JOIN coin_market_cap_meta m
        ON m.id = p.asset_id
    WHERE
        p.provider = 'coinmarketcap'
        AND recorded_at :: DATE < '2022-07-20'
        AND m.token_address IS NOT NULL
        AND p.platform IS NOT NULL
),
ibc_cg AS (
    SELECT
        CASE
            WHEN COALESCE(
                b.address,
                A.token_address
            ) ILIKE 'ibc%' THEN 'ibc/' || SPLIT_PART(COALESCE(b.address, A.token_address), '/', 2)
            ELSE COALESCE(
                b.address,
                A.token_address
            )
        END AS token_address,
        LOWER(
            A.id
        ) AS id,
        LOWER(
            CASE
                WHEN A.symbol IS NOT NULL THEN A.symbol
                ELSE CASE
                    A.id
                    WHEN 'cerberus-2' THEN 'CRBRUS'
                    WHEN 'cheqd-network' THEN 'CHEQ'
                    WHEN 'e-money-eur' THEN 'EEUR'
                    WHEN 'juno-network' THEN 'JUNO'
                    WHEN 'kujira' THEN 'KUJI'
                    WHEN 'medibloc' THEN 'MED'
                    WHEN 'microtick' THEN 'TICK'
                    WHEN 'neta' THEN 'NETA'
                    WHEN 'regen' THEN 'REGEN'
                    WHEN 'sommelier' THEN 'SOMM'
                    WHEN 'terra-luna' THEN 'LUNC'
                    WHEN 'umee' THEN 'UMEE'
                END
            END
        ) AS symbol,
        'cosmos' AS platform,
        'coingecko' AS provider,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__asset_metadata_coin_gecko'
        ) }} A
        LEFT JOIN {{ source(
            'osmosis_silver',
            'asset_metadata'
        ) }}
        b
        ON LOWER(
            A.symbol
        ) = LOWER(
            b.project_name
        )
    WHERE
        (
            id IN (
                'pstake-finance',
                'e-money-eur',
                'juno-network',
                'terra-luna',
                'cerberus-2',
                'hash-token',
                'sommelier',
                'assetmantle',
                'microtick',
                'regen',
                'galaxer',
                'bootleg-nft',
                'terrausd',
                'umee',
                'cmdx',
                'terra-krw',
                'cheqd-network',
                'neta',
                'medibloc',
                'kujira',
                'likecoin',
                'dig-chain',
                'hope-galaxy',
                'wrapped-bitcoin',
                'comdex',
                'darcmatter-coin',
                'ixo',
                'osmosis',
                'persistence',
                'stakeeasy-bjuno',
                'stakeeasy-juno-derivative',
                'stride-staked-atom',
                'seasy',
                'cosmos',
                'crescent-network',
                'crypto-com-chain',
                'injective-protocol',
                'arable-protocol',
                'inter-stable-token',
                'weth',
                'usdx',
                'odin-protocol',
                'chihuahua-token',
                'agoric',
                'stargaze',
                'lum-network',
                'starname',
                'ki',
                'graviton',
                'e-money',
                'fetch-ai',
                'axelar',
                'racoon',
                'posthuman',
                'sentinel',
                'stride',
                'usk',
                'dai',
                'ion',
                'iris-network',
                'evmos',
                'desmos',
                'akash-network',
                'osmosis'
            )
            OR token_address ILIKE 'ibc%'
        )
        AND (
            COALESCE(
                b.address,
                A.token_address
            ) ILIKE 'ibc%'
            OR b.address IN (
                'uosmo',
                'uion'
            )
        )
),
ibc_cmc AS (
    SELECT
        CASE
            WHEN COALESCE(
                b.address,
                A.token_address
            ) ILIKE 'ibc%' THEN 'ibc/' || SPLIT_PART(COALESCE(b.address, A.token_address), '/', 2)
            ELSE COALESCE(
                b.address,
                A.token_address
            )
        END AS token_address,
        LOWER(
            A.id
        ) AS id,
        LOWER(
            A.symbol
        ) AS symbol,
        'cosmos' AS platform,
        'coinmarketcap' AS provider,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__asset_metadata_coin_market_cap'
        ) }} A
        LEFT JOIN {{ source(
            'osmosis_silver',
            'asset_metadata'
        ) }}
        b
        ON LOWER(
            A.symbol
        ) = LOWER(
            b.project_name
        )
    WHERE
        (
            id IN (
                7431,
                7281,
                3874,
                3635,
                7271,
                4679,
                3794,
                14973,
                17748,
                2909,
                8905,
                13314,
                19899,
                18051,
                8279,
                17338,
                3773,
                4315,
                5604,
                9908,
                5835,
                2620,
                4263,
                17208,
                9480,
                3717,
                2396,
                12220,
                7226,
                17799,
                4846,
                14299,
                17451,
                22630,
                328,
                19640,
                14713,
                13877,
                22669,
                12256,
                19111,
                19938,
                11646,
                4172,
                9388,
                17183,
                7129,
                18699,
                16389,
                19686,
                9546,
                2643,
                5590,
                3408,
                16697,
                20381,
                2303,
                21781,
                16842
            )
            OR token_address ILIKE 'ibc%'
        )
        AND (
            COALESCE(
                b.address,
                A.token_address
            ) ILIKE 'ibc%'
            OR b.address IN (
                'uosmo',
                'uion'
            )
        )
),
ibc_am AS (
    SELECT
        address AS token_address,
        address AS id,
        project_name AS symbol,
        'cosmos' AS platform,
        'onchain' AS provider,
        '2000-01-01' AS _inserted_timestamp
    FROM
        {{ source(
            'osmosis_silver',
            'asset_metadata'
        ) }}
),
all_sources AS (
    SELECT
        LOWER(token_address) AS token_address,
        id,
        symbol,
        platform,
        provider,
        _inserted_timestamp
    FROM
        coin_gecko_meta
    WHERE
        token_address NOT ILIKE 'ibc%'
    UNION
    SELECT
        LOWER(token_address) AS token_address,
        id,
        symbol,
        platform,
        provider,
        _inserted_timestamp
    FROM
        coin_market_cap_meta
    UNION
    SELECT
        LOWER(token_address) AS token_address,
        id,
        symbol,
        platform,
        provider,
        _inserted_timestamp
    FROM
        legacy_coin_gecko_meta
    UNION
    SELECT
        LOWER(token_address) AS token_address,
        id,
        symbol,
        platform,
        provider,
        _inserted_timestamp
    FROM
        legacy_coin_market_cap_meta
    UNION
    SELECT
        token_address,
        id,
        symbol,
        platform,
        provider,
        _inserted_timestamp
    FROM
        ibc_cg
    UNION
    SELECT
        token_address,
        id,
        symbol,
        platform,
        provider,
        _inserted_timestamp
    FROM
        ibc_cmc
    UNION
    SELECT
        token_address,
        id,
        symbol,
        platform,
        provider,
        _inserted_timestamp
    FROM
        ibc_am
),
FINAL AS (
    SELECT
        token_address,
        id,
        symbol,
        CASE
            WHEN platform IN (
                'arbitrum-nova',
                'arbitrum-one',
                'arbitrum'
            ) THEN 'arbitrum'
            WHEN platform IN ('avalanche') THEN 'avalanche'
            WHEN platform IN (
                'binance-smart-chain',
                'binancecoin',
                'bnb'
            ) THEN 'bsc'
            WHEN platform IN ('ethereum') THEN 'ethereum'
            WHEN platform IN (
                'gnosis',
                'xdai'
            ) THEN 'gnosis'
            WHEN platform IN (
                'optimism',
                'optimistic-ethereum'
            ) THEN 'optimism'
            WHEN platform IN (
                'polygon',
                'polygon-pos'
            ) THEN 'polygon'
            WHEN platform IN (
                'cosmos',
                'evmos',
                'osmosis',
                'terra',
                'terra-2'
            ) THEN 'cosmos'
            WHEN LOWER(platform) = 'algorand' THEN 'algorand'
            ELSE NULL
        END AS blockchain,
        --supported chains only
        provider,
        _inserted_timestamp
    FROM
        all_sources
    WHERE
        token_address IS NOT NULL
        AND blockchain IS NOT NULL

{% if is_incremental() %}
AND token_address || blockchain NOT IN (
    SELECT
        DISTINCT token_address || blockchain
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    CASE
        WHEN token_address ILIKE 'ibc%' THEN token_address
        ELSE LOWER(token_address)
    END AS token_address,
    id,
    symbol,
    blockchain,
    provider,
    {{ dbt_utils.surrogate_key(
        ['token_address','id','symbol','blockchain','provider']
    ) }} AS _unique_key,
    _inserted_timestamp
FROM
    FINAL
WHERE
    len(token_address) > 0
    AND NOT (
        blockchain IN (
            'arbitrum',
            'avalanche',
            'bsc',
            'ethereum',
            'gnosis',
            'optimism',
            'polygon'
        )
        AND token_address NOT ILIKE '0x%'
    )
    AND NOT (
        blockchain = 'algorand'
        AND TRY_CAST(
            token_address AS INT
        ) IS NULL
    ) qualify(ROW_NUMBER() over (PARTITION BY token_address, id, symbol, blockchain, provider
ORDER BY
    _inserted_timestamp DESC)) = 1
