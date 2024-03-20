{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'platform'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['prices']
) }}

WITH base_assets AS (
    -- get all asset metdata

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
    -- get all assets currently supported
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
    -- make generic adjustments to asset metadata
    SELECT
        LOWER(
            CASE
                WHEN LENGTH(TRIM(id)) <= 0 THEN NULL
                ELSE TRIM(id)
            END
        ) AS id_adj,
        CASE
            WHEN TRIM(
                A.token_address
            ) ILIKE 'http%' THEN IFF(
                LENGTH(
                    TRIM(
                        REGEXP_SUBSTR(REGEXP_SUBSTR(TRIM(A.token_address), '[^/]+$'), '^[-a-zA-Z0-9./_]+'),
                        '-/_'
                    )
                ) <= 1,
                NULL,
                TRIM(
                    REGEXP_SUBSTR(REGEXP_SUBSTR(TRIM(A.token_address), '[^/]+$'), '^[-a-zA-Z0-9./_]+'),
                    '-/_'
                )
            )
            ELSE IFF(
                LENGTH(
                    TRIM(
                        REGEXP_SUBSTR(TRIM(A.token_address), '^[-a-zA-Z0-9./_]+'),
                        '-/_'
                    )
                ) <= 1,
                NULL,
                TRIM(
                    REGEXP_SUBSTR(TRIM(A.token_address), '^[-a-zA-Z0-9./_]+'),
                    '-/_'
                )
            )
        END AS token_address_adj,
        CASE
            WHEN LENGTH(TRIM(NAME)) <= 0 THEN NULL
            ELSE TRIM(NAME)
        END AS name_adj,
        CASE
            WHEN LENGTH(TRIM(symbol)) <= 0 THEN NULL
            ELSE TRIM(symbol)
        END AS symbol_adj,
        LOWER(
            CASE
                WHEN LENGTH(TRIM(A.platform)) <= 0 THEN NULL
                ELSE TRIM(
                    A.platform
                )
            END
        ) AS platform_adj,
        source,
        CASE
            WHEN C.token_address IS NOT NULL THEN FALSE
            ELSE TRUE
        END AS is_deprecated,
        A._inserted_timestamp
    FROM
        base_assets A
        LEFT JOIN current_supported_assets C
        ON A.token_address = C.token_address
        AND A.platform = C.platform
    WHERE
        token_address_adj IS NOT NULL
        AND platform_adj IS NOT NULL
),
solana_adj AS (
    --add solana specific adjustments and tokens
    SELECT
        A.id_adj,
        COALESCE(
            s.token_address,
            A.token_address_adj
        ) AS token_address_adj,
        A.name_adj,
        A.symbol_adj,
        'solana' AS platform_adj,
        'solana' AS source,
        A.is_deprecated,
        A._inserted_timestamp
    FROM
        base_adj A
        INNER JOIN {{ source(
            'solana_silver',
            'token_metadata'
        ) }}
        s
        ON A.id_adj = LOWER(TRIM(s.coin_market_cap_id))
    WHERE
        token_address_adj NOT ILIKE '%-%'
),
ibc_adj AS (
    --add ibc specific adjustments and tokens
    SELECT
        A.id_adj,
        CASE
            WHEN COALESCE(
                i.address,
                A.token_address_adj
            ) ILIKE 'ibc%' THEN 'ibc/' || SPLIT_PART(COALESCE(i.address, A.token_address_adj), '/', 2)
            ELSE COALESCE(
                i.address,
                A.token_address_adj
            )
        END AS token_address_adj,
        A.name_adj,
        CASE
            A.id_adj
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
        END AS symbol_adj,
        'cosmos' AS platform_adj,
        'ibc' AS source,
        A.is_deprecated,
        A._inserted_timestamp
    FROM
        base_adj A
        INNER JOIN {{ source(
            'osmosis_silver',
            'asset_metadata'
        ) }}
        i
        ON LOWER(
            A.symbol_adj
        ) = LOWER(
            i.project_name
        )
    WHERE
        (
            A.id_adj IN (
                SELECT
                    id
                FROM
                    {{ ref('silver__ibc_asset_metadata') }}
                WHERE
                    provider = 'coingecko'
            )
            OR A.token_address_adj ILIKE 'ibc%'
        )
        AND (
            COALESCE(
                i.address,
                A.token_address_adj
            ) ILIKE 'ibc%'
            OR i.address IN (
                'uosmo',
                'uion'
            )
        )
),
all_assets AS (
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
        solana_adj
    UNION ALL
    SELECT
        *
    FROM
        ibc_adj
)
SELECT
    id_adj AS id,
    token_address_adj AS token_address,
    name_adj AS NAME,
    symbol_adj AS symbol,
    platform_adj AS platform,
    source,
    is_deprecated,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['token_address','platform']) }} AS token_asset_metadata_coin_gecko_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_assets qualify(ROW_NUMBER() over (PARTITION BY token_address, platform
ORDER BY
    _inserted_timestamp DESC)) = 1 -- built for tokens with token_address (not native/gas tokens)
