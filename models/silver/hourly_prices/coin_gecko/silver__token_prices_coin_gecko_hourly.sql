{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['recorded_hour::DATE'],
) }}

WITH date_hours AS (

    SELECT
        date_hour
    FROM
        {{ ref(
            'core__dim_date_hours'
        ) }}
    WHERE
        date_hour >= '2020-05-05'
        AND date_hour <= (
            SELECT
                MAX(recorded_hour)
            FROM
                {{ ref(
                    'silver__hourly_prices_coin_gecko'
                ) }}
        )

{% if is_incremental() %}
AND date_hour > (
    SELECT
        MAX(recorded_hour)
    FROM
        {{ this }}
)
{% endif %}
),
asset_metadata AS (
    SELECT
        DISTINCT REGEXP_SUBSTR(REGEXP_REPLACE(token_address, '^x', '0x'), '0x[a-zA-Z0-9]*') AS token_address,
        id,
        symbol,
        LOWER(platform :: STRING) AS platform
    FROM
        {{ ref(
            'silver__asset_metadata_coin_gecko'
        ) }}
),
base_date_hours_address AS (
    SELECT
        date_hour,
        token_address,
        id,
        symbol,
        platform
    FROM
        date_hours
        CROSS JOIN asset_metadata
),
base_legacy_prices AS (
    SELECT
        DATE_TRUNC(
            'hour',
            p.recorded_at
        ) AS recorded_hour,
        m.token_address,
        p.asset_id AS id,
        COALESCE(p.symbol,m.symbol) AS symbol,
        LOWER(p.platform :name :: STRING) AS platform,
        AVG(
            p.price
        ) AS CLOSE --returns single price if multiple prices within the 59th minute
    FROM
        {{ source(
            'legacy_db',
            'prices_v2'
        ) }}
        p
        LEFT JOIN asset_metadata m
        ON m.id = p.asset_id
    WHERE
        provider = 'coingecko'
        AND MINUTE(recorded_at) = 59
        AND recorded_at :: DATE < '2022-08-24'

{% if is_incremental() %}
AND recorded_at > (
    SELECT
        MAX(recorded_hour)
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1,
    2,
    3,
    4,
    5
),
base_prices AS (
    SELECT
        p.recorded_hour,
        p._inserted_timestamp,
        m.token_address,
        p.id,
        m.symbol,
        m.platform,
        p.close
    FROM
        {{ ref(
            'silver__hourly_prices_coin_gecko'
        ) }}
        p
        LEFT JOIN asset_metadata m
        ON m.id = p.id
    WHERE
        recorded_hour :: DATE >= '2022-08-24'

{% if is_incremental() %}
AND p._inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
prices AS (
    SELECT
        recorded_hour,
        token_address,
        id,
        symbol,
        platform,
        close
    FROM
        base_legacy_prices
    UNION
    SELECT
        recorded_hour,
        token_address,
        id,
        symbol,
        platform,
        close
    FROM
        base_prices
),
imputed_prices AS (
    SELECT
        d.date_hour,
        d.token_address,
        d.id,
        d.symbol,
        d.platform,
        p.close AS hourly_close,
        LAST_VALUE(
            p.close ignore nulls
        ) over (
            PARTITION BY d.token_address
            ORDER BY
                d.date_hour rows unbounded preceding
        ) AS imputed_close
    FROM
        base_date_hours_address d
        LEFT JOIN prices p
        ON p.recorded_hour = d.date_hour
        AND p.token_address = d.token_address
        AND LOWER(
                p.id
            ) = LOWER(
                d.id
            )
        
),
final_prices AS (
    SELECT
        p.date_hour AS recorded_hour,
        p.token_address,
        p.id,
        p.symbol,
        p.platform,
        COALESCE(
            p.hourly_close,
            p.imputed_close
        ) AS CLOSE,
        CASE
            WHEN p.hourly_close IS NULL THEN TRUE
            ELSE FALSE
        END AS imputed
    FROM
        imputed_prices p
    WHERE
        CLOSE IS NOT NULL
),
base_timestamp AS (
SELECT
    f.recorded_hour,
    f.token_address,
    f.platform,
    f.symbol,
    AVG(f.CLOSE) AS CLOSE,
    CASE
        WHEN (CAST(ARRAY_AGG(imputed) AS STRING)) ILIKE '%true%' THEN TRUE
        ELSE FALSE 
    END AS imputed,
    {{ dbt_utils.surrogate_key( ['f.recorded_hour','f.token_address','f.platform','f.symbol'] ) }} AS _unique_key,
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    final_prices f
LEFT JOIN base_prices b ON f.recorded_hour = b.recorded_hour 
    AND f.token_address = b.token_address 
    AND f.platform = b.platform
GROUP BY
    1,
    2,
    3,
    4
),
FINAL AS (

SELECT
    recorded_hour,
    token_address,
    symbol,
    platform,
    close,
    imputed,
    _unique_key,
    _inserted_timestamp,
    LAST_VALUE(
            _inserted_timestamp ignore nulls
        ) over (
            PARTITION BY token_address
            ORDER BY
                recorded_hour rows unbounded preceding
        ) AS imputed_timestamp
FROM base_timestamp
)

SELECT
    recorded_hour,
    token_address,
    symbol,
    platform,
    close,
    imputed,
    _unique_key,
    CASE
        WHEN imputed_timestamp IS NULL THEN '2022-08-23'
        ELSE COALESCE(_inserted_timestamp,imputed_timestamp)
    END AS _inserted_timestamp
FROM final