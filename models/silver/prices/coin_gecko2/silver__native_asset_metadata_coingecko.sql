{{ config(
    materialized = 'incremental',
    unique_key = ['token_asset_metadata_coin_gecko_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['prices']
) }}

WITH base_assets AS (
    -- get all native asset metdata

    SELECT
        id,
        token_address,
        NAME,
        symbol,
        platform,
        platform_id,
        source,
        _inserted_timestamp
    FROM
        {{ ref('bronze__all_asset_metadata_coingecko2') }}
    WHERE
        id IN (
            SELECT
                id
            FROM
                {{ ref('silver__native_asset_metadata') }}
            WHERE
                provider = 'coingecko'
        )

{% if is_incremental() %}
AND _inserted_timestamp > (
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
        platform_id,
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
        LOWER(
            CASE
                WHEN LENGTH(TRIM(A.platform_id)) <= 0 THEN NULL
                ELSE TRIM(
                    A.platform_id
                )
            END
        ) AS platform_id_adj,
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
        AND A.platform_id = C.platform_id
),
all_assets AS (
    SELECT
        id_adj AS id,
        token_address_adj AS token_address,
        name_adj AS NAME,
        symbol_adj AS symbol,
        platform_adj AS platform,
        platform_id_adj AS platform_id,
        source,
        is_deprecated,
        _inserted_timestamp
    FROM
        base_adj
)
SELECT
    id,
    token_address,
    NAME,
    symbol,
    platform,
    platform_id,
    source,
    is_deprecated,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['LOWER(token_address)','platform_id']) }} AS token_asset_metadata_coin_gecko_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_assets
WHERE
    token_address IS NOT NULL
    AND platform IS NOT NULL
    AND platform_id IS NOT NULL qualify(ROW_NUMBER() over (PARTITION BY LOWER(token_address), platform_id
ORDER BY
    _inserted_timestamp DESC)) = 1 -- built for tokens with token_address (not native/gas tokens)
