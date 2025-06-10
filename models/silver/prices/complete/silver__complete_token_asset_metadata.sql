{{ config(
    materialized = 'incremental',
    unique_key = ['complete_token_asset_metadata_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['blockchain'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_id, token_address, symbol, name),SUBSTRING(asset_id, token_address, symbol, name)",
    tags = ['prices','heal']
) }}

WITH asset_metadata AS (

    SELECT
        token_address,
        id AS asset_id,
        UPPER(COALESCE(C.symbol, A.symbol)) AS symbol,
        COALESCE(
            C.name,
            A.name
        ) AS NAME,
        decimals,
        A.blockchain,
        blockchain_name,
        blockchain_id,
        is_deprecated,
        A.provider,
        A.source,
        A._inserted_timestamp
    FROM
        {{ ref('silver__token_asset_metadata_priority') }} A
        LEFT JOIN {{ ref('core__dim_contracts') }} C
        ON LOWER(
            C.address
        ) = LOWER(
            A.token_address
        )
        AND C.blockchain = A.blockchain

{% if is_incremental() %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
heal_model AS (
    SELECT
        t0.token_address,
        asset_id,
        C.symbol AS symbol_heal,
        C.name AS name_heal,
        C.decimals AS decimals_heal,
        t0.blockchain,
        blockchain_name,
        blockchain_id,
        is_deprecated,
        provider,
        source,
        t0._inserted_timestamp
    FROM
        {{ this }}
        t0
        LEFT JOIN {{ ref('core__dim_contracts') }} C
        ON LOWER(
            C.address
        ) = LOWER(
            t0.token_address
        )
        AND C.blockchain = t0.blockchain
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
                        {{ ref('core__dim_contracts') }} C
                    WHERE
                        C.modified_timestamp > DATEADD('DAY', -90, SYSDATE())
                        AND C.decimals IS NOT NULL
                        AND LOWER(
                            C.address
                        ) = LOWER(
                            t1.token_address
                        )
                        AND C.blockchain = t1.blockchain
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
                        {{ ref('core__dim_contracts') }} C
                    WHERE
                        C.modified_timestamp > DATEADD('DAY', -90, SYSDATE())
                        AND C.symbol IS NOT NULL
                        AND LOWER(
                            C.address
                        ) = LOWER(
                            t2.token_address
                        )
                        AND C.blockchain = t2.blockchain
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
        asset_metadata

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
UNION ALL
SELECT
    token_address,
    asset_id,
    symbol_heal AS symbol,
    name_heal AS NAME,
    decimals_heal AS decimals,
    blockchain,
    blockchain_name,
    blockchain_id,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp
FROM
    heal_model
{% endif %}
),
final_final AS (
    SELECT
        token_address,
        asset_id,
        symbol,
        NAME,
        decimals,
        blockchain,
        blockchain_name,
        blockchain_id,
        is_deprecated,
        provider,
        source,
        _inserted_timestamp
    FROM
        FINAL
    UNION ALL
    SELECT
        token_address,
        id AS asset_id,
        symbol,
        NAME,
        decimals,
        blockchain,
        blockchain_name,
        blockchain_id,
        is_deprecated,
        provider,
        source,
        _inserted_timestamp
    FROM
        {{ ref('silver__token_asset_metadata_enhanced') }} A

{% if is_incremental() %}
WHERE
    A.modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    A.*,
    COALESCE(
        b.is_verified,
        FALSE
    ) AS is_verified,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['LOWER(a.token_address)','a.blockchain']) }} AS complete_token_asset_metadata_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    final_final A
    LEFT JOIN {{ ref('silver__tokens_enhanced') }}
    b
    ON LOWER(
        A.token_address
    ) = LOWER(
        b.address
    )
    AND A.blockchain = b.blockchain qualify(ROW_NUMBER() over (PARTITION BY LOWER(A.token_address), A.blockchain
ORDER BY
    _inserted_timestamp DESC)) = 1
