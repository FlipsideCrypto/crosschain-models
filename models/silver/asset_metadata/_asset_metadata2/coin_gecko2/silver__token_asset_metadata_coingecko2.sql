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
        p.value :: STRING AS token_address,
        NAME,
        symbol,
        p.key :: STRING AS platform,
        source,
        _inserted_timestamp
    FROM
        {{ ref('silver__all_asset_metadata_coingecko2') }} A,
        LATERAL FLATTEN(
            input => VALUE :platforms
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
    --get all assets currently supported by coingecko
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
            WHEN A.id = 'osmosis' THEN 'uosmo'
            WHEN A.id = 'algorand' THEN '0'
            ELSE A.token_address
        END AS token_address_adj,
        A.name AS name_adj,
        A.symbol AS symbol_adj,
        LOWER(
            CASE
                WHEN A.id = 'osmosis' THEN 'osmosis'
                WHEN A.id = 'algorand' THEN 'algorand'
                WHEN s.token_address IS NOT NULL THEN 'solana'
                ELSE A.platform :: STRING
            END
        ) AS platform_adj,
        CASE
            WHEN s.token_address IS NOT NULL THEN 'solana'
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
            'solana_silver',
            'token_metadata'
        ) }}
        s
        ON LOWER(
            A.token_address
        ) = LOWER(
            s.token_address
        )
    WHERE
        (
            platform_adj = 'solana'
            AND token_address_adj NOT ILIKE '0x%'
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
        ) AS symbol_adj,
        'cosmos' AS platform_adj,
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
        provider = 'coingecko')
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
),
final_adj AS (
    SELECT
        id_adj,
        TRIM(
            CASE
                WHEN A.token_address_adj ILIKE 'http%' THEN SPLIT_PART(
                    A.token_address_adj,
                    '/',
                    5
                )
                WHEN A.token_address_adj ILIKE '%:%' THEN SPLIT_PART(
                    A.token_address_adj,
                    ':',
                    1
                )
                WHEN A.token_address_adj ILIKE '%(%'
                OR A.token_address_adj ILIKE '% %' THEN SPLIT_PART(
                    A.token_address_adj,
                    ' ',
                    1
                )
                ELSE A.token_address_adj
            END,
            '"'
        ) AS token_address_adj,
        name_adj,
        symbol_adj,
        platform_adj,
        source_adj,
        is_deprecated,
        _inserted_timestamp
    FROM
        all_assets_adj A
    WHERE
        token_address_adj IS NOT NULL
        AND LENGTH(token_address_adj) > 0
        AND platform_adj IS NOT NULL
        AND LENGTH(platform_adj) > 0
)
SELECT
    id_adj AS id,
    CASE
        WHEN token_address_adj ILIKE 'ibc%'
        OR platform_adj = 'solana' THEN token_address_adj
        ELSE LOWER(token_address_adj)
    END AS token_address,
    CASE
        WHEN LENGTH(name_adj) <= 0 THEN NULL
        ELSE name_adj
    END AS NAME,
    CASE
        WHEN LENGTH(symbol_adj) <= 0 THEN NULL
        ELSE symbol_adj
    END AS symbol,
    platform_adj AS platform,
    source_adj AS source,
    is_deprecated,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['token_address','platform']) }} AS token_asset_metadata_coin_gecko_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    final_adj qualify(ROW_NUMBER() over (PARTITION BY token_address, platform
ORDER BY
    _inserted_timestamp DESC)) = 1 -- specifically built for tokens with token_address (not native/gas tokens)
