{{ config(
    materialized = 'incremental',
    unique_key = ['complete_token_asset_metadata_id'],
    incremental_strategy = 'delete+insert',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(token_address, blockchain)",
    tags = ['prices']
) }}

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
    A._inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['LOWER(token_address)','A.blockchain']) }} AS complete_token_asset_metadata_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__token_asset_metadata_priority2') }} A
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

qualify(ROW_NUMBER() over (PARTITION BY LOWER(A.token_address), A.blockchain
ORDER BY
    A._inserted_timestamp DESC)) = 1
