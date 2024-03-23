{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
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
    GREATEST(COALESCE(A.inserted_timestamp, '2000-01-01'), COALESCE(C.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(A.modified_timestamp, '2000-01-01'), COALESCE(C.modified_timestamp, '2000-01-01')) AS modified_timestamp,
    token_asset_metadata_priority_id AS ez_asset_metadata_id
FROM
    {{ ref('silver__token_asset_metadata_priority2') }} A
    LEFT JOIN {{ ref('core__dim_contracts') }} C
    ON LOWER(
        C.address
    ) = LOWER(
        A.token_address
    )
    AND C.blockchain = A.blockchain
