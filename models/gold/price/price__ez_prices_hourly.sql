{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'PRICES, METADATA',
    } } },
    tags = ['prices']
) }}

SELECT
    HOUR,
    CASE
        WHEN p.token_address ILIKE 'ibc%'
        OR blockchain IN (
            'solana',
            'bitcoin',
            'flow',
            'aptos',
            'ton',
            'stellar'
        ) THEN p.token_address
        ELSE LOWER(
            p.token_address
        )
    END AS token_address,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    FALSE AS is_native,
    is_imputed,
    is_deprecated,
    is_verified,
    inserted_timestamp,
    modified_timestamp,
    complete_token_prices_id AS ez_prices_hourly_id
FROM
    {{ ref('silver__complete_token_prices') }}
    p
UNION ALL
SELECT
    HOUR,
    NULL AS token_address,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    TRUE AS is_native,
    is_imputed,
    is_deprecated,
    TRUE AS is_verified,
    inserted_timestamp,
    modified_timestamp,
    complete_native_prices_id AS ez_prices_hourly_id
FROM
    {{ ref('silver__complete_native_prices') }}
