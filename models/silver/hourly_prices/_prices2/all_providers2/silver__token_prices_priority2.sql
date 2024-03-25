{{ config(
    materialized = 'incremental',
    unique_key = ['hour','token_address','blockchain'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['hour::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(token_address, hour, blockchain)",
    tags = ['prices']
) }}

WITH priority_prices AS (

    SELECT
        HOUR :: TIMESTAMP AS HOUR,
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

qualify(ROW_NUMBER() over (PARTITION BY HOUR, token_address, blockchain
ORDER BY
    priority ASC, id ASC, blockchain_id ASC nulls last, _inserted_timestamp DESC)) = 1
),
token_asset_metadata AS (
    SELECT
        id,
        token_address,
        blockchain,
        blockchain_id,
        provider,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__token_asset_metadata_priority2'
        ) }}
),
date_hours AS (
    SELECT
        date_hour :: TIMESTAMP AS date_hour,
        token_address,
        blockchain,
        blockchain_id,
        id,
        provider
    FROM
        {{ ref('core__dim_date_hours') }}
        CROSS JOIN token_asset_metadata
    WHERE
        date_hour <= (
            SELECT
                MAX(HOUR)
            FROM
                {{ ref('silver__token_prices_all_providers2') }}
        )

{% if is_incremental() %}
AND date_hour >= (
    SELECT
        MIN(HOUR)
    FROM
        {{ this }}
)
{% endif %}
),
latest_supported_assets AS (
    SELECT
        token_address,
        blockchain,
        DATE_TRUNC('hour', MAX(_inserted_timestamp)) AS last_supported_timestamp
    FROM
        token_asset_metadata
    GROUP BY
        1,
        2),
        imputed_prices AS (
            SELECT
                d.date_hour,
                d.token_address,
                d.blockchain,
                d.blockchain_id,
                CASE
                    WHEN d.date_hour <= s.last_supported_timestamp THEN p.price
                    ELSE NULL
                END AS hourly_price,
                CASE
                    WHEN hourly_price IS NOT NULL THEN NULL
                    WHEN hourly_price IS NULL
                    AND d.date_hour <= s.last_supported_timestamp THEN LAST_VALUE(
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
                d.id,
                d.provider,
                CASE
                    WHEN imputed_price IS NOT NULL THEN 'imputed'
                    ELSE p.source
                END AS source,
                CASE
                    WHEN imputed_price IS NOT NULL THEN 7
                    ELSE p.priority
                END AS priority,
                last_supported_timestamp,
                CASE
                    WHEN imputed_price IS NOT NULL THEN SYSDATE()
                    ELSE p._inserted_timestamp
                END AS _inserted_timestamp
            FROM
                date_hours d
                LEFT JOIN priority_prices p
                ON d.date_hour = p.hour
                AND d.token_address = p.token_address
                AND d.blockchain = p.blockchain
                LEFT JOIN latest_supported_assets s
                ON s.token_address = d.token_address
                AND s.blockchain = d.blockchain
        )
    SELECT
        date_hour AS HOUR,
        token_address,
        blockchain,
        blockchain_id,
        final_price AS price,
        imputed AS is_imputed,
        id,
        provider,
        priority,
        source,
        last_supported_timestamp,
        _inserted_timestamp,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['hour','token_address','blockchain']) }} AS token_prices_priority_hourly_id,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        imputed_prices
    WHERE
        final_price IS NOT NULL
