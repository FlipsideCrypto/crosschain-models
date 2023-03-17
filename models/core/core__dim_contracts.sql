{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH base AS (
SELECT
    address,
    symbol,
    NAME,
    decimals,
    contract_metadata,
    'ethereum' AS blockchain
FROM
    {{ source(
        'ethereum_silver',
        'contracts') }}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    contract_metadata,
    'optimism' AS blockchain
FROM
    {{ source(
        'optimism_silver',
        'contracts') }}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    contract_metadata,
    'arbitrum' AS blockchain
FROM
    {{ source(
        'arbitrum_silver',
        'contracts') }}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    contract_metadata,
    'polygon' AS blockchain
FROM
    {{ source(
        'polygon_silver',
        'contracts') }}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    contract_metadata,
    'avalanche' AS blockchain
FROM
    {{ source(
        'avalanche_silver',
        'contracts') }}
)

SELECT
    address,
    symbol,
    NAME,
    decimals,
    contract_metadata,
    blockchain
FROM base
