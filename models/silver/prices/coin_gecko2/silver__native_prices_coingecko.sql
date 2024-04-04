-- depends_on: {{ ref('core__dim_date_hours') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['native_prices_coingecko_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE'],
    tags = ['prices']
) }}

WITH base_prices AS (
    -- get all prices and join to asset metadata

    SELECT
        p.recorded_hour,
        m.symbol,
        p.id,
        m.platform,
        p.close,
        p.source,
        p._inserted_timestamp
    FROM
        {{ ref(
            'bronze__all_prices_coingecko2'
        ) }}
        p
        INNER JOIN {{ ref(
            'silver__native_asset_metadata_coingecko'
        ) }}
        m
        ON m.id = LOWER(TRIM(p.id))
    WHERE
        p.close <> 0
        AND p.recorded_hour :: DATE <> '1970-01-01'

{% if is_incremental() %}
AND p._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
latest_supported_assets AS (
    -- get the latest supported timestamp for each asset
    SELECT
        symbol,
        platform,
        DATE_TRUNC('hour', MAX(_inserted_timestamp)) AS last_supported_timestamp
    FROM
        {{ ref(
            'silver__native_asset_metadata_coingecko'
        ) }}
    GROUP BY
        1,
        2
)

{% if is_incremental() %},
price_gaps AS (
    -- identify missing prices by id and platform
    SELECT
        symbol,
        platform,
        recorded_hour,
        prev_recorded_hour,
        gap
    FROM
        (
            SELECT
                symbol,
                platform,
                recorded_hour,
                LAG(
                    recorded_hour,
                    1
                ) over (
                    PARTITION BY id,
                    platform
                    ORDER BY
                        recorded_hour ASC
                ) AS prev_RECORDED_HOUR,
                DATEDIFF(
                    HOUR,
                    prev_RECORDED_HOUR,
                    recorded_hour
                ) - 1 AS gap
            FROM
                {{ this }}
        )
    WHERE
        gap > 0
),
native_asset_metadata AS (
    -- get all metadata for assets with missing prices
    SELECT
        symbol,
        id,
        platform,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__native_asset_metadata_coingecko'
        ) }}
    WHERE
        CONCAT(
            symbol,
            '-',
            platform
        ) IN (
            SELECT
                CONCAT(
                    symbol,
                    '-',
                    platform
                )
            FROM
                price_gaps
        )
),
date_hours AS (
    -- generate spine of all possible hours, between gaps
    SELECT
        date_hour,
        symbol,
        id,
        platform
    FROM
        {{ ref('core__dim_date_hours') }}
        CROSS JOIN native_asset_metadata
    WHERE
        date_hour <= (
            SELECT
                MAX(recorded_hour)
            FROM
                price_gaps
        )
        AND date_hour >= (
            SELECT
                MIN(prev_recorded_hour)
            FROM
                price_gaps
        )
),
imputed_prices AS (
    -- impute missing prices
    SELECT
        d.date_hour,
        d.symbol,
        d.id,
        d.platform,
        CASE
            WHEN d.date_hour <= s.last_supported_timestamp THEN p.close
            ELSE NULL
        END AS hourly_price,
        CASE
            WHEN hourly_price IS NULL
            AND d.date_hour <= s.last_supported_timestamp THEN LAST_VALUE(
                hourly_price ignore nulls
            ) over (
                PARTITION BY d.symbol,
                d.platform
                ORDER BY
                    d.date_hour rows BETWEEN unbounded preceding
                    AND CURRENT ROW
            )
            ELSE NULL
        END AS imputed_price,
        CASE
            WHEN imputed_price IS NOT NULL THEN TRUE
            ELSE p.is_imputed
        END AS imputed,
        COALESCE(
            hourly_price,
            imputed_price
        ) AS final_price,
        CASE
            WHEN imputed_price IS NOT NULL THEN 'imputed_cg'
            ELSE p.source
        END AS source,
        CASE
            WHEN imputed_price IS NOT NULL THEN SYSDATE()
            ELSE p._inserted_timestamp
        END AS _inserted_timestamp
    FROM
        date_hours d
        LEFT JOIN {{ this }}
        p
        ON d.symbol = p.symbol
        AND d.platform = p.platform
        AND d.date_hour = p.recorded_hour
        LEFT JOIN latest_supported_assets s
        ON d.symbol = s.symbol
        AND d.platform = s.platform
)
{% endif %},
FINAL AS (
    SELECT
        p.recorded_hour,
        p.symbol,
        p.id,
        p.platform,
        CASE
            WHEN p.recorded_hour <= s.last_supported_timestamp THEN p.close
            ELSE NULL
        END AS close_price,
        -- only include prices during supported ranges
        FALSE AS is_imputed,
        p.source,
        p._inserted_timestamp
    FROM
        base_prices p
        LEFT JOIN latest_supported_assets s
        ON p.symbol = s.symbol
        AND p.platform = s.platform
    WHERE
        close_price IS NOT NULL

{% if is_incremental() %}
UNION ALL
SELECT
    date_hour AS recorded_hour,
    symbol,
    id,
    platform,
    final_price AS close_price,
    imputed AS is_imputed,
    source,
    _inserted_timestamp
FROM
    imputed_prices
WHERE
    close_price IS NOT NULL
    AND is_imputed
{% endif %}
)
SELECT
    recorded_hour,
    symbol,
    id,
    platform,
    close_price AS CLOSE,
    is_imputed,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['recorded_hour','symbol','platform']) }} AS native_prices_coingecko_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY recorded_hour, symbol, platform
ORDER BY
    _inserted_timestamp DESC)) = 1
