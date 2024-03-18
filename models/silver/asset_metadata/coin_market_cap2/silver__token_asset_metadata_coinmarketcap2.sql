{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'platform'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['prices']
) }}

WITH base_assets AS (

    SELECT
        id,
        p.this :token_address :: STRING AS token_address,
        NAME,
        symbol,
        p.this :name :: STRING AS platform,
        p.this :id :: STRING AS platform_id,
        p.this :slug :: STRING AS platform_slug,
        p.this :symbol :: STRING AS platform_symbol,
        source,
        _inserted_timestamp,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['token_address','platform']) }} AS asset_metadata_coin_market_cap_id,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ ref('silver__all_asset_metadata_coinmarketcap2') }} A,
        LATERAL FLATTEN(
            input => VALUE :platform
        ) p

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
current_supported_assets AS (
    --get all assets currently supported by coinmarketcap
    SELECT
        token_address,
        platform,
        _inserted_timestamp
    FROM
        base_assets
    WHERE
        _inserted_timestamp = (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                base_assets
        )
),
base_adj AS (
    SELECT
        LOWER(
            A.id
        ) AS id_adj,
        CASE
            WHEN LOWER(
                A.platform
            ) = 'aptos' THEN A.token_address
            WHEN TRIM(
                A.token_address
            ) ILIKE '^x%'
            OR TRIM(
                A.token_address
            ) ILIKE '0x%' THEN REGEXP_SUBSTR(REGEXP_REPLACE(A.token_address, '^x', '0x'), '0x[a-zA-Z0-9]*')
            WHEN A.id = '12220' THEN 'uosmo'
            WHEN A.id = '4030' THEN '0'
            WHEN A.token_address ILIKE 'https%' THEN SPLIT_PART(
                A.token_address,
                '/',
                5
            )
            ELSE A.token_address
        END AS token_address_adj,
        A.name AS name_adj,
        A.symbol AS symbol_adj,
        LOWER(
            CASE
                WHEN A.id = 'osmosis' THEN 'osmosis'
                WHEN A.id = 'algorand' THEN 'algorand'
                ELSE A.platform :: STRING
            END
        ) AS platform_adj,
        platform_id,
        platform_slug,
        platform_symbol,
        source AS source_adj,
        CASE
            WHEN C.token_address IS NOT NULL THEN FALSE
            ELSE TRUE
        END AS is_deprecated,
        A._inserted_timestamp
    FROM
        base_assets A
        LEFT JOIN current_supported_assets C
        ON LOWER(
            A.token_address
        ) = LOWER(
            C.token_address
        )
        AND LOWER(
            A.platform
        ) = LOWER(
            C.platform
        )
),
ibc_adj AS (
    SELECT
        LOWER(
            A.id
        ) AS id_adj,
        CASE
            WHEN COALESCE(
                i.address,
                A.token_address
            ) ILIKE 'ibc%' THEN 'ibc/' || SPLIT_PART(COALESCE(i.address, A.token_address), '/', 2)
            ELSE COALESCE(
                i.address,
                A.token_address
            )
        END AS token_address_adj,
        A.name AS name_adj,
        LOWER(
            A.symbol
        ) AS symbol_adj,
        'cosmos' AS platform_adj,
        platform_id,
        platform_slug,
        platform_symbol,
        CASE
            WHEN i.project_name IS NOT NULL THEN 'ibc'
            ELSE source
        END AS source_adj,
        CASE
            WHEN C.token_address IS NOT NULL THEN FALSE
            ELSE TRUE
        END AS is_deprecated,
        A._inserted_timestamp
    FROM
        base_assets A
        LEFT JOIN current_supported_assets C
        ON LOWER(
            A.token_address
        ) = LOWER(
            C.token_address
        )
        AND LOWER(
            A.platform
        ) = LOWER(
            C.platform
        )
        LEFT JOIN {{ source(
            'osmosis_silver',
            'asset_metadata'
        ) }}
        i
        ON LOWER(
            A.symbol
        ) = LOWER(
            i.project_name
        )
    WHERE
        (LOWER(A.id) IN (
    SELECT
        id
    FROM
        {{ ref('silver__ibc_asset_metadata') }}
    WHERE
        provider = 'coinmarketcap')
        OR A.token_address ILIKE 'ibc%')
        AND (
            COALESCE(
                i.address,
                A.token_address
            ) ILIKE 'ibc%'
            OR i.address IN (
                'uosmo',
                'uion'
            )
        )
),
sol_adj AS(
    SELECT
        LOWER(
            A.id
        ) AS id_adj,
        CASE
            WHEN A.token_address ILIKE 'https%' THEN SPLIT_PART(
                A.token_address,
                '/',
                5
            )
            ELSE A.token_address
        END AS token_address_adj,
        A.name AS name_adj,
        LOWER(
            A.symbol
        ) AS symbol_adj,
        'solana' AS platform_adj,
        platform_id,
        platform_slug,
        platform_symbol,
        'solana' AS source_adj,
        CASE
            WHEN C.token_address IS NOT NULL THEN FALSE
            ELSE TRUE
        END AS is_deprecated,
        A._inserted_timestamp
    FROM
        base_assets A
        LEFT JOIN current_supported_assets C
        ON LOWER(
            A.token_address
        ) = LOWER(
            C.token_address
        )
        AND LOWER(
            A.platform
        ) = LOWER(
            C.platform
        )
        INNER JOIN {{ source(
            'solana_silver',
            'token_metadata'
        ) }}
        s
        ON A.id = s.coin_market_cap_id
),
all_assets_adj AS (
    SELECT
        *
    FROM
        base_adj
    WHERE
        token_address_adj NOT ILIKE 'ibc%'
    UNION ALL
    SELECT
        *
    FROM
        ibc_adj
    UNION ALL
    SELECT
        *
    FROM
        sol_adj
),
FINAL AS (
    SELECT
        id_adj AS id,
        CASE
            WHEN token_address_adj ILIKE 'ibc%'
            OR platform_adj = 'solana' THEN token_address_adj
            ELSE LOWER(token_address_adj)
        END AS token_address,
        name_adj AS NAME,
        symbol_adj AS symbol,
        platform_adj AS platform,
        platform_id,
        platform_slug,
        platform_symbol,
        source_adj AS source,
        is_deprecated,
        _inserted_timestamp
    FROM
        all_assets_adj
    WHERE
        token_address IS NOT NULL
        AND LENGTH(token_address) > 0
        AND platform IS NOT NULL
        AND LENGTH(platform) > 0
)
SELECT
    id,
    token_address,
    CASE
        WHEN LENGTH(NAME) <= 0 THEN NULL
        ELSE NAME
    END AS NAME,
    CASE
        WHEN LENGTH(symbol) <= 0 THEN NULL
        ELSE symbol
    END AS symbol,
    platform,
    platform_id,
    platform_slug,
    platform_symbol,
    source,
    is_deprecated,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['token_address','platform']) }} AS token_asset_metadata_coin_market_cap_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY token_address, platform
ORDER BY
    _inserted_timestamp DESC)) = 1 -- specifically built for tokens with token_address (not native/gas tokens)
