{{ config(
    materialized = 'incremental',
    unique_key = ['complete_token_asset_metadata_id'],
    incremental_strategy = 'delete+insert',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(token_address, blockchain)",
    tags = ['prices']
) }}

SELECT
    token_address,
    id,
    COALESCE(
        C.symbol,
        A.symbol
    ) AS symbol,
    COALESCE(
        C.name,
        A.name
    ) AS NAME,
    decimals,
    A.blockchain,
    blockchain_id,
    is_deprecated,
    A.provider,
    A._inserted_timestamp,
    GREATEST(COALESCE(A.inserted_timestamp, '2000-01-01'), COALESCE(C.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(A.modified_timestamp, '2000-01-01'), COALESCE(C.modified_timestamp, '2000-01-01')) AS modified_timestamp,
    token_asset_metadata_priority_id AS complete_token_asset_metadata_id
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

qualify(ROW_NUMBER() over (PARTITION BY A.token_address, A.blockchain
ORDER BY
    A._inserted_timestamp DESC)) = 1
