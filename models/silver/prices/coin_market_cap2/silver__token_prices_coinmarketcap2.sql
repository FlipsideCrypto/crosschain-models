-- depends_on: {{ ref('core__dim_date_hours') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['token_prices_coinmarketcap_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE'],
    tags = ['prices']
) }}

WITH base_prices AS (
     -- get all prices and join to asset metadata
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
            'bronze__all_prices_coinmarketcap2'
        ) }}
        p
        INNER JOIN {{ ref(
            'silver__token_asset_metadata_coinmarketcap2'
        ) }}
        m
        ON m.id = LOWER(TRIM(p.id))
    WHERE
        p.close <> 0
        AND p.recorded_hour :: DATE <> '1970-01-01'
        AND m.token_address IS NOT NULL
        AND m.platform_id IS NOT NULL

{% if is_incremental() %}
AND p._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)

{% if is_incremental() %},
price_gaps AS (
    -- identify missing prices by token_address and platform_id
    SELECT
        token_address,
        platform_id,
        recorded_hour,
        prev_recorded_hour,
        gap
    FROM
        (
            SELECT
                token_address,
                platform_id,
                recorded_hour,
                LAG(
                    recorded_hour,
                    1
                ) over (PARTITION BY LOWER(token_address), platform_id
            ORDER BY
                recorded_hour ASC) AS prev_RECORDED_HOUR,
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
token_asset_metadata AS (
    -- get all token metadata for tokens with missing prices
    SELECT
        token_address,
        id,
        platform,
        platform_id
    FROM
        {{ ref(
            'silver__token_asset_metadata_coinmarketcap2'
        ) }}
    WHERE
        CONCAT(LOWER(token_address), '-', platform_id) IN (
            SELECT
                CONCAT(LOWER(token_address), '-', platform_id)
            FROM
                price_gaps)
        ),
        date_hours AS (
            -- generate spine of all possible hours, between gaps
            SELECT
                date_hour,
                token_address,
                id,
                platform,
                platform_id
            FROM
                {{ ref('core__dim_date_hours') }}
                CROSS JOIN token_asset_metadata
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
                d.token_address,
                d.id,
                d.platform,
                d.platform_id,
                p.close AS hourly_price,
                CASE
                    WHEN hourly_price IS NULL THEN LAST_VALUE(
                        hourly_price ignore nulls
                    ) over (
                        PARTITION BY d.token_address,
                        d.platform_id
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
                    WHEN imputed_price IS NOT NULL THEN 'imputed'
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
                ON LOWER(
                    d.token_address
                ) = LOWER(
                    p.token_address
                )
                AND d.platform_id = p.platform_id
                AND d.date_hour = p.recorded_hour
        )
    {% endif %},
    FINAL AS (
        SELECT
            recorded_hour,
            token_address,
            id,
            platform,
            platform_id,
            CLOSE,
            FALSE AS is_imputed,
            source,
            _inserted_timestamp
        FROM
            base_prices

{% if is_incremental() %}
UNION ALL
SELECT
    date_hour AS recorded_hour,
    token_address,
    id,
    platform,
    platform_id,
    final_price AS CLOSE,
    imputed AS is_imputed,
    source,
    _inserted_timestamp
FROM
    imputed_prices
WHERE
    final_price IS NOT NULL
    AND is_imputed
{% endif %}
)
SELECT
    recorded_hour,
    token_address,
    id,
    platform,
    platform_id,
    CLOSE,
    is_imputed,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['recorded_hour','LOWER(token_address)','platform_id']) }} AS token_prices_coinmarketcap_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY recorded_hour, LOWER(token_address), platform_id
ORDER BY
    _inserted_timestamp DESC)) = 1

