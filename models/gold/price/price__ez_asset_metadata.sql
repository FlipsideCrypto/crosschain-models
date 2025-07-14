{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'PRICES, METADATA',
    } } },
    tags = ['prices']
) }}

SELECT
    CASE
        WHEN A.token_address ILIKE 'ibc%'
        OR blockchain IN (
            'solana',
            'bitcoin',
            'flow',
            'aptos',
            'ton',
            'stellar'
        ) THEN A.token_address
        ELSE LOWER(
            A.token_address
        )
    END AS token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    FALSE AS is_native,
    is_deprecated,
    is_verified,
    is_verified_modified_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_token_asset_metadata_id AS ez_asset_metadata_id
FROM
    {{ ref('silver__complete_token_asset_metadata') }} A
UNION ALL
SELECT
    NULL AS token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    TRUE AS is_native,
    is_deprecated,
    TRUE AS is_verified,
    NULL AS is_verified_modified_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_native_asset_metadata_id AS ez_asset_metadata_id
FROM
    {{ ref('silver__complete_native_asset_metadata') }}
