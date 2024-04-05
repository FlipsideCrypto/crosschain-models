{{ config(
    materialized = 'incremental',
    unique_key = ['native_asset_metadata_coinmarketcap_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['prices']
) }}

WITH base_assets AS (
    -- get all native asset metdata

    SELECT
        A.id,
        LOWER(
            A.name
        ) AS NAME,
        LOWER(
            A.symbol
        ) AS symbol,
        decimals,
        source,
        _inserted_timestamp
    FROM
        {{ ref('bronze__all_asset_metadata_coinmarketcap2') }} A
        INNER JOIN {{ ref('silver__native_assets_seed') }}
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
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    OR A.id NOT IN (
        SELECT
            DISTINCT id
        FROM
            {{ this }}
    ) --load all data for new assets
{% endif %}
),
current_supported_assets AS (
    -- get all assets currently supported
    SELECT
        symbol,
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
            WHEN LENGTH(TRIM(A.name)) <= 0 THEN NULL
            ELSE TRIM(
                A.name
            )
        END AS name_adj,
        CASE
            WHEN LENGTH(TRIM(A.symbol)) <= 0 THEN NULL
            ELSE TRIM(
                A.symbol
            )
        END AS symbol_adj,
        decimals,
        source,
        CASE
            WHEN C.symbol IS NOT NULL THEN FALSE
            ELSE TRUE
        END AS is_deprecated,
        A._inserted_timestamp
    FROM
        base_assets A
        LEFT JOIN current_supported_assets C
        ON A.symbol = C.symbol
)
SELECT
    id_adj AS id,
    name_adj AS NAME,
    symbol_adj AS symbol,
    decimals,
    source,
    is_deprecated,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['symbol']) }} AS native_asset_metadata_coinmarketcap_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_adj
WHERE
    symbol IS NOT NULL
    AND NAME IS NOT NULL qualify(ROW_NUMBER() over (PARTITION BY symbol
ORDER BY
    _inserted_timestamp DESC)) = 1 -- built for native assets