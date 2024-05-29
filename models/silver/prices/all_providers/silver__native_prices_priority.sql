-- depends_on: {{ ref('core__dim_date_hours') }}
-- depends_on: {{ ref('silver__native_asset_metadata_priority') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['native_prices_priority_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(symbol, recorded_hour, blockchain)",
    tags = ['prices']
) }}

WITH priority_prices AS (
    -- get all prices and qualify by priority

    SELECT
        recorded_hour,
        symbol,
        NAME,
        id,
        decimals,
        blockchain,
        price,
        is_imputed,
        provider,
        CASE
            WHEN provider = 'coingecko'
            AND is_imputed = FALSE THEN 1
            WHEN provider = 'coinmarketcap'
            AND is_imputed = FALSE THEN 2
            WHEN provider = 'coingecko'
            AND is_imputed = TRUE THEN 3
            WHEN provider = 'coinmarketcap'
            AND is_imputed = TRUE THEN 4
        END AS priority,
        source,
        _inserted_timestamp
    FROM
        {{ ref('silver__native_prices_all_providers') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    OR modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    OR symbol NOT IN (
        SELECT
            DISTINCT symbol
        FROM
            {{ this }}
    ) --load all data for new assets
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY recorded_hour, symbol
ORDER BY
    priority ASC, id ASC, blockchain ASC, _inserted_timestamp DESC, modified_timestamp DESC)) = 1
)

{% if is_incremental() %},
price_gaps AS (
    -- identify missing prices by symbol, gaps most likely to exist between providers
    SELECT
        symbol,
        recorded_hour,
        prev_recorded_hour,
        gap
    FROM
        (
            SELECT
                symbol,
                recorded_hour,
                LAG(
                    recorded_hour,
                    1
                ) over (
                    PARTITION BY symbol,
                    blockchain
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
    -- get all token metadata for tokens with missing prices
    SELECT
        symbol,
        NAME,
        decimals,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__native_asset_metadata_priority'
        ) }}
    WHERE
        symbol IN (
            SELECT
                symbol
            FROM
                price_gaps
        )
),
latest_supported_assets AS (
    --get the latest supported timestamp for each asset with missing prices
    SELECT
        symbol,
        DATE_TRUNC('hour', MAX(_inserted_timestamp)) AS last_supported_timestamp
    FROM
        native_asset_metadata
    GROUP BY
        1),
        date_hours AS (
            SELECT
                date_hour,
                symbol,
                NAME,
                decimals
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
                d.name,
                d.decimals,
                CASE
                    WHEN d.date_hour <= s.last_supported_timestamp THEN p.price
                    ELSE NULL
                END AS hourly_price,
                CASE
                    WHEN hourly_price IS NULL
                    AND (
                        d.date_hour <= s.last_supported_timestamp
                    ) THEN LAST_VALUE(
                        hourly_price ignore nulls
                    ) over (
                        PARTITION BY d.symbol
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
                    WHEN imputed_price IS NOT NULL THEN LAST_VALUE(
                        p.blockchain ignore nulls
                    ) over (
                        PARTITION BY d.symbol
                        ORDER BY
                            d.date_hour rows BETWEEN unbounded preceding
                            AND CURRENT ROW
                    )
                    ELSE p.blockchain
                END AS blockchain,
                CASE
                    WHEN imputed_price IS NOT NULL THEN LAST_VALUE(
                        p.id ignore nulls
                    ) over (
                        PARTITION BY d.symbol
                        ORDER BY
                            d.date_hour rows BETWEEN unbounded preceding
                            AND CURRENT ROW
                    )
                    ELSE p.id
                END AS id,
                CASE
                    WHEN imputed_price IS NOT NULL THEN LAST_VALUE(
                        p.provider ignore nulls
                    ) over (
                        PARTITION BY d.symbol
                        ORDER BY
                            d.date_hour rows BETWEEN unbounded preceding
                            AND CURRENT ROW
                    )
                    ELSE p.provider
                END AS provider,
                CASE
                    WHEN imputed_price IS NOT NULL THEN 'imputed_priority'
                    ELSE p.source
                END AS source,
                CASE
                    WHEN imputed_price IS NOT NULL THEN 7
                    ELSE p.priority
                END AS priority,
                CASE
                    WHEN imputed_price IS NOT NULL THEN SYSDATE()
                    ELSE p._inserted_timestamp
                END AS _inserted_timestamp
            FROM
                date_hours d
                LEFT JOIN {{ this }}
                p
                ON LOWER(
                    d.symbol
                ) = LOWER(
                    p.symbol
                )
                AND d.date_hour = p.recorded_hour
                LEFT JOIN latest_supported_assets s
                ON LOWER(
                    d.symbol
                ) = LOWER(
                    s.symbol
                )
        )
    {% endif %},
    FINAL AS (
        SELECT
            recorded_hour,
            symbol,
            NAME,
            decimals,
            blockchain,
            price,
            is_imputed,
            id,
            provider,
            priority,
            source,
            _inserted_timestamp
        FROM
            priority_prices

{% if is_incremental() %}
UNION ALL
SELECT
    date_hour AS recorded_hour,
    symbol,
    NAME,
    decimals,
    blockchain,
    final_price AS price,
    imputed AS is_imputed,
    id,
    provider,
    priority,
    source,
    _inserted_timestamp
FROM
    imputed_prices
WHERE
    price IS NOT NULL
    AND is_imputed
{% endif %}
)
SELECT
    recorded_hour,
    symbol,
    NAME,
    decimals,
    blockchain,
    price,
    is_imputed,
    id,
    provider,
    priority,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['recorded_hour','symbol']) }} AS native_prices_priority_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL

{% if is_incremental() %}
qualify(ROW_NUMBER() over (PARTITION BY recorded_hour, symbol
ORDER BY
    priority ASC, id ASC, blockchain ASC, _inserted_timestamp DESC)) = 1
{% endif %}
