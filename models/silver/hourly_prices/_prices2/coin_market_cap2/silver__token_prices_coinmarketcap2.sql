{{ config(
    materialized = 'incremental',
    unique_key = ['recorded_hour','token_address','platform_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE'],
    tags = ['prices']
) }}

WITH token_asset_metadata AS (
    --get all assets

    SELECT
        id,
        token_address,
        platform,
        platform_id,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__token_asset_metadata_coinmarketcap2'
        ) }}
),
base_hours_metadata AS (
    --generate spine of all possible hours up to the latest supported hour
    SELECT
        date_hour,
        id,
        token_address,
        platform,
        platform_id
    FROM
        {{ ref(
            'core__dim_date_hours'
        ) }}
        CROSS JOIN token_asset_metadata
    WHERE
        date_hour <= (
            SELECT
                MAX(recorded_hour)
            FROM
                {{ ref(
                    'silver__all_prices_coinmarketcap2'
                ) }}
        )

{% if is_incremental() %}
AND date_hour >= (
    SELECT
        MAX(recorded_hour) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
),
base_prices AS (
    --get all prices and join to asset metadata
    SELECT
        p.recorded_hour,
        m.token_address,
        p.id,
        m.platform,
        m.platform_id,
        p.close,
        p.source,
        p._inserted_timestamp
    FROM
        {{ ref(
            'silver__all_prices_coinmarketcap2'
        ) }}
        p
        LEFT JOIN token_asset_metadata m
        ON m.id = LOWER(TRIM(p.id))
    WHERE
        p.close <> 0
        AND p.recorded_hour :: DATE <> '1970-01-01'

{% if is_incremental() %}
AND p._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
),
latest_supported_assets AS (
    --get the latest supported timestamp for each asset
    SELECT
        token_address,
        platform_id,
        DATE_TRUNC('hour', MAX(_inserted_timestamp)) AS last_supported_timestamp
    FROM
        token_asset_metadata
    GROUP BY
        1,
        2),
        imputed_prices AS (
            --impute missing prices, ensuring no gaps
            SELECT
                d.date_hour,
                d.token_address,
                d.id,
                d.platform,
                d.platform_id,
                CASE
                    WHEN d.date_hour <= s.last_supported_timestamp THEN p.close
                    ELSE NULL
                END AS hourly_close,
                CASE
                    WHEN hourly_close IS NOT NULL THEN NULL
                    WHEN hourly_close IS NULL
                    AND d.date_hour <= s.last_supported_timestamp THEN LAST_VALUE(
                        hourly_close ignore nulls
                    ) over (
                        PARTITION BY d.token_address,
                        d.platform_id
                        ORDER BY
                            d.date_hour rows BETWEEN unbounded preceding
                            AND CURRENT ROW
                    )
                    ELSE NULL
                END AS imputed_close,
                --only impute prices for coinmarketcap supported ranges
                COALESCE(
                    hourly_close,
                    imputed_close
                ) AS final_close,
                CASE
                    WHEN imputed_close IS NULL THEN FALSE
                    ELSE TRUE
                END AS imputed,
                CASE
                    WHEN imputed THEN 'imputed'
                    ELSE p.source
                END AS source,
                s.last_supported_timestamp,
                p._inserted_timestamp
            FROM
                base_hours_metadata d
                LEFT JOIN base_prices p
                ON p.recorded_hour = d.date_hour
                AND p.token_address = d.token_address
                AND p.platform_id = d.platform_id
                LEFT JOIN latest_supported_assets s
                ON s.token_address = d.token_address
                AND s.platform_id = d.platform_id
        ),
        final_prices AS (
            SELECT
                DATEADD(
                    HOUR,
                    1,
                    date_hour
                ) AS recorded_hour,
                --roll the close price forward 1 hour
                token_address,
                id,
                platform,
                platform_id,
                final_close AS CLOSE,
                imputed,
                source,
                last_supported_timestamp,
                _inserted_timestamp AS _inserted_timestamp_raw,
                CASE
                    WHEN imputed THEN SYSDATE()
                    ELSE NULL
                END AS _imputed_timestamp
            FROM
                imputed_prices
            WHERE
                CLOSE IS NOT NULL
        )
    SELECT
        recorded_hour,
        token_address,
        id,
        platform,
        platform_id,
        CLOSE,
        imputed,
        source,
        last_supported_timestamp,
        COALESCE(
            _inserted_timestamp_raw,
            _imputed_timestamp
        ) AS _inserted_timestamp,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['recorded_hour','token_address','platform_id']) }} AS token_prices_coin_market_cap_hourly_id,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        final_prices qualify(ROW_NUMBER() over (PARTITION BY recorded_hour, token_address, platform_id
    ORDER BY
        _inserted_timestamp DESC)) = 1
