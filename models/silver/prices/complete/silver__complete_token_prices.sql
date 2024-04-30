{{ config(
    materialized = 'incremental',
    unique_key = ['complete_token_prices_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['hour::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(token_address, hour, blockchain)",
    tags = ['prices']
) }}

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
    p._inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['HOUR','LOWER(p.token_address)','p.blockchain']) }} AS complete_token_prices_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__token_prices_priority2') }}
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
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY LOWER(p.token_address), p.blockchain, HOUR
ORDER BY
    p._inserted_timestamp DESC, p.modified_timestamp DESC)) = 1
