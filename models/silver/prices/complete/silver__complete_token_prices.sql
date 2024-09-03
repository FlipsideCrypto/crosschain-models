{{ config(
    materialized = 'incremental',
    unique_key = ['complete_token_prices_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['hour::DATE','blockchain'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_id, token_address, symbol, name),SUBSTRING(asset_id, token_address, symbol, name)",
    tags = ['prices','heal']
) }}

WITH prices AS (

    SELECT
        DATEADD(
            HOUR,
            1,
            recorded_hour
        ) AS HOUR,
        --roll the close price forward 1 hour
        p.token_address,
        p.id AS asset_id,
        UPPER(symbol) AS symbol,
        NAME,
        decimals,
        price,
        p.blockchain,
        p.blockchain_name,
        p.blockchain_id,
        is_imputed,
        CASE
            WHEN m.is_deprecated IS NULL THEN FALSE
            ELSE m.is_deprecated
        END AS is_deprecated,
        p.provider,
        p.source,
        p._inserted_timestamp
    FROM
        {{ ref('silver__token_prices_priority') }}
        p
        LEFT JOIN {{ ref('silver__complete_token_asset_metadata') }}
        m
        ON LOWER(
            p.token_address
        ) = LOWER(
            m.token_address
        )
        AND p.blockchain = m.blockchain

{% if is_incremental() %}
WHERE
    p._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    OR p.modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    ) qualify(ROW_NUMBER() over (PARTITION BY LOWER(p.token_address), p.blockchain, HOUR
ORDER BY
    p._inserted_timestamp DESC, p.modified_timestamp DESC)) = 1
{% endif %}
),

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
heal_model AS (
    SELECT
        HOUR,
        t0.token_address,
        t0.asset_id,
        UPPER(
            m.symbol
        ) AS symbol_heal,
        m.name AS name_heal,
        m.decimals AS decimals_heal,
        price,
        t0.blockchain,
        t0.blockchain_name,
        t0.blockchain_id,
        is_imputed,
        t0.is_deprecated,
        t0.provider,
        t0.source,
        t0._inserted_timestamp
    FROM
        {{ this }}
        t0
        LEFT JOIN {{ ref('silver__complete_token_asset_metadata') }}
        m
        ON LOWER(
            t0.token_address
        ) = LOWER(
            m.token_address
        )
        AND t0.blockchain = m.blockchain
    WHERE
        CONCAT(LOWER(t0.token_address), '-', t0.blockchain) IN (
            SELECT
                CONCAT(LOWER(t1.token_address), '-', t1.blockchain)
            FROM
                {{ this }}
                t1
            WHERE
                t1.decimals IS NULL
                AND t1._inserted_timestamp < (
                    SELECT
                        MAX(
                            _inserted_timestamp
                        )
                    FROM
                        {{ this }}
                )
                AND EXISTS (
                    SELECT
                        1
                    FROM
                        {{ ref('silver__complete_token_asset_metadata') }}
                        m
                    WHERE
                        m.modified_timestamp > DATEADD('DAY', -14, SYSDATE())
                        AND m.decimals IS NOT NULL
                        AND LOWER(
                            m.token_address
                        ) = LOWER(
                            t1.token_address
                        )
                        AND m.blockchain = t1.blockchain
                )
            GROUP BY
                1
        )
        OR CONCAT(LOWER(t0.token_address), '-', t0.blockchain) IN (
            SELECT
                CONCAT(LOWER(t2.token_address), '-', t2.blockchain)
            FROM
                {{ this }}
                t2
            WHERE
                t2.symbol IS NULL
                AND t2._inserted_timestamp < (
                    SELECT
                        MAX(
                            _inserted_timestamp
                        )
                    FROM
                        {{ this }}
                )
                AND EXISTS (
                    SELECT
                        1
                    FROM
                        {{ ref('silver__complete_token_asset_metadata') }}
                        m
                    WHERE
                        m.modified_timestamp > DATEADD('DAY', -14, SYSDATE())
                        AND m.symbol IS NOT NULL
                        AND LOWER(
                            m.token_address
                        ) = LOWER(
                            t2.token_address
                        )
                        AND m.blockchain = t2.blockchain
                )
            GROUP BY
                1
        )
),
{% endif %}

FINAL AS (
    SELECT
        *
    FROM
        prices

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
UNION ALL
SELECT
    HOUR,
    token_address,
    asset_id,
    symbol_heal AS symbol,
    name_heal AS NAME,
    decimals_heal AS decimals,
    price,
    blockchain,
    blockchain_name,
    blockchain_id,
    is_imputed,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp
FROM
    heal_model
{% endif %}
)
SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['HOUR','LOWER(token_address)','blockchain']) }} AS complete_token_prices_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY LOWER(token_address), blockchain, HOUR
ORDER BY
    _inserted_timestamp DESC)) = 1
