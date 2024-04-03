{{ config(
    materialized = 'incremental',
    unique_key = ['native_asset_metadata_coingecko_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['prices']
) }}

WITH base_assets AS (
    -- get all native asset metdata

    SELECT
        A.id,
        A.name,
        A.symbol,
        n.platform,
        source,
        _inserted_timestamp
    FROM
        {{ ref('bronze__all_asset_metadata_coingecko2') }} A
        INNER JOIN {{ ref('silver__native_asset_metadata') }}
        n
        ON LOWER(
            A.id
        ) = LOWER(
            n.id
        )
        AND LOWER(
            A.name
        ) = LOWER(
            n.name
        )
        AND LOWER(
            A.symbol
        ) = LOWER(n.symbol)

{% if is_incremental() %}
AND _inserted_timestamp >= (
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
        id,
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
                WHEN LENGTH(TRIM(A.id)) <= 0 THEN NULL
                ELSE TRIM(
                    A.id
                )
            END
        ) AS id_adj,
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
            WHEN C.id IS NOT NULL THEN FALSE
            ELSE TRUE
        END AS is_deprecated,
        A._inserted_timestamp
    FROM
        base_assets A
        LEFT JOIN current_supported_assets C
        ON A.id = C.id
        AND A.platform = C.platform
)
SELECT
    id_adj AS id,
    name_adj AS NAME,
    symbol_adj AS symbol,
    platform_adj AS platform,
    source,
    is_deprecated,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['id','platform']) }} AS native_asset_metadata_coingecko_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_adj
WHERE
    id IS NOT NULL
    AND platform IS NOT NULL qualify(ROW_NUMBER() over (PARTITION BY id, platform
ORDER BY
    _inserted_timestamp DESC)) = 1 -- built for native assets
