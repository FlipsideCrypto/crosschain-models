{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'platform'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH base_assets AS (

    SELECT
        id,
        p.value :: STRING AS token_address,
        NAME,
        symbol,
        p.key :: STRING AS platform,
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
assets_adj AS (
    SELECT
        LOWER(id) AS id_adj,
        LOWER(
            CASE
                WHEN id_adj = 'osmosis' THEN 'osmosis'
                WHEN id_adj = 'algorand' THEN 'algorand'
                WHEN s.token_address IS NOT NULL THEN 'solana'
                WHEN (
                    id IN (
                        SELECT
                            id
                        FROM
                            {{ ref('silver__ibc_asset_metadata_coingecko') }}
                    )
                    OR A.token_address ILIKE 'ibc%'
                )
                AND (COALESCE(i.address, A.token_address) ILIKE 'ibc%'
                OR i.address IN ('uosmo', 'uion')) THEN 'cosmos'
                ELSE platform :: STRING
            END
        ) AS platform_adj,
        CASE
            WHEN platform_adj = 'aptos' THEN A.token_address
            WHEN id_adj = 'osmosis' THEN 'uosmo'
            WHEN id_adj = 'algorand' THEN '0'
            WHEN COALESCE(
                i.address,
                A.token_address
            ) ILIKE 'ibc%' THEN 'ibc/' || SPLIT_PART(COALESCE(i.address, A.token_address), '/', 2)
            WHEN TRIM(
                A.token_address
            ) ILIKE '^x%'
            OR TRIM(
                A.token_address
            ) ILIKE '0x%' THEN REGEXP_SUBSTR(REGEXP_REPLACE(A.token_address, '^x', '0x'), '0x[a-zA-Z0-9]*')
            ELSE A.token_address
        END AS token_address_adj,
        NAME,
        CASE
            WHEN A.symbol IS NULL
            AND platform_adj = 'cosmos' THEN CASE
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
            ELSE A.symbol
        END AS symbol,
        A._inserted_timestamp
    FROM
        base_assets A
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
        A.token_address IS NOT NULL
        AND LENGTH(
            A.token_address
        ) > 0
        AND token_address_adj IS NOT NULL
        AND LENGTH(token_address_adj) > 0
        AND platform IS NOT NULL
        AND LENGTH(platform) > 0
        AND platform_adj IS NOT NULL
        AND LENGTH(platform_adj) > 0
)
SELECT
    id_adj AS id,
    CASE
        WHEN token_address_adj ILIKE 'ibc%'
        OR platform_adj = 'solana' THEN token_address_adj
        ELSE LOWER(token_address)
    END AS token_address,
    NAME,
    symbol,
    platform_adj AS platform,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['token_address_adj','platform_adj']) }} AS asset_metadata_coin_gecko_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    assets_adj qualify(ROW_NUMBER() over (PARTITION BY token_address_adj, platform_adj
ORDER BY
    _inserted_timestamp DESC)) = 1 -- specifically built for tokens with token_address (not native/gas tokens)
