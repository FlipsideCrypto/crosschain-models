{{ config(
    materialized = 'incremental',
    unique_key = ['recorded_hour','token_address','platform'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE']
) }}

WITH date_hours AS (

    SELECT
        date_hour
    FROM
        {{ ref(
            'core__dim_date_hours'
        ) }}
    WHERE
        date_hour >= '2018-01-01'
        AND date_hour <= (
            SELECT
                MAX(recorded_hour)
            FROM
                {{ ref(
                    'silver__hourly_prices_coin_gecko2'
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
asset_metadata AS (
    SELECT
        DISTINCT CASE
            WHEN LOWER(platform) = 'aptos' THEN token_address
            WHEN TRIM(token_address) ILIKE '^x%'
            OR TRIM(token_address) ILIKE '0x%' THEN REGEXP_SUBSTR(REGEXP_REPLACE(token_address, '^x', '0x'), '0x[a-zA-Z0-9]*')
            WHEN id = 'osmosis' THEN 'uosmo'
            WHEN id = 'algorand' THEN '0'
            ELSE token_address
        END AS token_address_adj,
        id,
        LOWER(
            CASE
                WHEN id = 'osmosis' THEN 'osmosis'
                WHEN id = 'algorand' THEN 'algorand'
                WHEN id = 'solana' THEN 'solana'
                ELSE platform :: STRING
            END
        ) AS platform_adj,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__asset_metadata_coin_gecko2'
        ) }}
        qualify(ROW_NUMBER() over (PARTITION BY token_address_adj, platform_adj
    ORDER BY
        _inserted_timestamp DESC)) = 1
),
base_date_hours_address AS (
    SELECT
        date_hour,
        token_address_adj AS token_address,
        id,
        platform_adj AS platform
    FROM
        date_hours
        CROSS JOIN asset_metadata
),
base_prices AS (
    SELECT
        p.recorded_hour,
        m.token_address_adj AS token_address,
        p.id,
        m.platform_adj AS platform,
        p.close,
        p._inserted_timestamp
    FROM
        {{ ref(
            'silver__hourly_prices_coin_gecko2'
        ) }}
        p
        LEFT JOIN asset_metadata m
        ON m.id = p.id

{% if is_incremental() %}
AND p._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
),
imputed_prices AS (
    SELECT
        --dateadd(hour,1,date_hour) AS date_hour, --if we want to roll the close price forward 1 hour
        date_hour AS recorded_hour,
        d.token_address,
        d.id,
        d.platform,
        p.close AS hourly_close,
        LAST_VALUE(
            p.close ignore nulls
        ) over (
            PARTITION BY d.token_address,
            d.platform
            ORDER BY
                d.date_hour rows unbounded preceding
        ) AS imputed_close,
        COALESCE(
            hourly_close,
            imputed_close
        ) AS final_close,
        CASE
            WHEN hourly_close IS NULL THEN TRUE
            ELSE FALSE
        END AS imputed,
        p._inserted_timestamp
    FROM
        base_date_hours_address d
        LEFT JOIN base_prices p
        ON p.recorded_hour = d.date_hour
        AND p.token_address = d.token_address
        AND p.platform = d.platform
),
final_prices AS (
    SELECT
        recorded_hour,
        token_address,
        id,
        platform,
        final_close AS CLOSE,
        imputed,
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
    platform,
    id,
    CLOSE,
    imputed,
    COALESCE(
        _inserted_timestamp_raw,
        _imputed_timestamp
    ) AS _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['recorded_hour','token_address','platform']) }} AS token_prices_coin_gecko_hourly_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    final_prices
