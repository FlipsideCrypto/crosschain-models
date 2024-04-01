-- depends_on: {{ ref('core__dim_date_hours') }}
-- depends_on: {{ ref('silver__token_asset_metadata_priority2') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['token_prices_priority_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(token_address, recorded_hour, blockchain)",
    tags = ['prices']
) }}

WITH priority_prices AS (
    -- get all prices and qualify by priority

    SELECT
        recorded_hour,
        token_address,
        blockchain,
        blockchain_id,
        price,
        is_imputed,
        id,
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
            WHEN provider = 'osmosis-pool-balance' THEN 5
            WHEN provider = 'osmosis-swap' THEN 6
        END AS priority,
        source,
        _inserted_timestamp
    FROM
        {{ ref('silver__token_prices_all_providers2') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY recorded_hour, LOWER(token_address), blockchain
ORDER BY
    priority ASC, id ASC, blockchain_id ASC nulls last, _inserted_timestamp DESC)) = 1)

{% if is_incremental() %},
price_gaps AS (
    -- identify missing prices by token_address and blockchain, gaps most likely to exist between providers
    SELECT
        token_address,
        blockchain,
        recorded_hour,
        prev_recorded_hour,
        gap
    FROM
        (
            SELECT
                token_address,
                blockchain,
                recorded_hour,
                LAG(
                    recorded_hour,
                    1
                ) over (PARTITION BY LOWER(token_address), blockchain
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
        blockchain
    FROM
        {{ ref(
            'silver__token_asset_metadata_priority2'
        ) }}
    WHERE
        CONCAT(LOWER(token_address), '-', blockchain) IN (
            SELECT
                CONCAT(LOWER(token_address), '-', blockchain)
            FROM
                price_gaps)
        ),
        date_hours AS (
            SELECT
                date_hour,
                token_address,
                blockchain
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
                d.blockchain,
                p.price AS hourly_price,
                CASE
                    WHEN hourly_price IS NULL THEN LAST_VALUE(
                        hourly_price ignore nulls
                    ) over (
                        PARTITION BY d.token_address,
                        d.blockchain
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
                        p.id ignore nulls
                    ) over (
                        PARTITION BY d.token_address,
                        d.blockchain
                        ORDER BY
                            d.date_hour rows BETWEEN unbounded preceding
                            AND CURRENT ROW
                    )
                    ELSE p.id
                END AS id,
                CASE
                    WHEN imputed_price IS NOT NULL THEN LAST_VALUE(
                        p.blockchain_id ignore nulls
                    ) over (
                        PARTITION BY d.token_address,
                        d.blockchain
                        ORDER BY
                            d.date_hour rows BETWEEN unbounded preceding
                            AND CURRENT ROW
                    )
                    ELSE p.blockchain_id
                END AS blockchain_id,
                CASE
                    WHEN imputed_price IS NOT NULL THEN LAST_VALUE(
                        p.provider ignore nulls
                    ) over (
                        PARTITION BY d.token_address,
                        d.blockchain
                        ORDER BY
                            d.date_hour rows BETWEEN unbounded preceding
                            AND CURRENT ROW
                    )
                    ELSE p.provider
                END AS provider,
                CASE
                    WHEN imputed_price IS NOT NULL THEN 'imputed'
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
                    d.token_address
                ) = LOWER(
                    p.token_address
                )
                AND d.blockchain = p.blockchain
                AND d.date_hour = p.recorded_hour
        )
    {% endif %},
    FINAL AS (
        SELECT
            recorded_hour,
            token_address,
            blockchain,
            blockchain_id,
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
    token_address,
    blockchain,
    blockchain_id,
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
    final_price IS NOT NULL
    AND is_imputed
{% endif %}
)
SELECT
    recorded_hour,
    token_address,
    blockchain,
    blockchain_id,
    price,
    is_imputed,
    id,
    provider,
    priority,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['recorded_hour','LOWER(token_address)','blockchain']) }} AS token_prices_priority_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL

{% if is_incremental() %}
qualify(ROW_NUMBER() over (PARTITION BY recorded_hour, LOWER(token_address), blockchain
ORDER BY
    priority ASC, id ASC, blockchain_id ASC nulls last, _inserted_timestamp DESC)) = 1
{% endif %}
