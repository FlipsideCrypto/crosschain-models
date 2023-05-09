{{ config(
    materialized = 'view'
) }}

SELECT
    contract_address,
    abi_hash,
    'ethereum' AS blockchain
FROM
    {{ source(
        'ethereum_silver',
        'abis'
    ) }}
union all

SELECT
    contract_address,
    abi_hash,
    'polygon' AS blockchain
FROM
    {{ source(
        'polygon_silver',
        'abis'
    ) }}

union all

SELECT
    contract_address,
    abi_hash,
    'avalanche' AS blockchain
FROM
    {{ source(
        'avalanche_silver',
        'abis'
    ) }}

union all

SELECT
    contract_address,
    abi_hash,
    'bsc' AS blockchain
FROM
    {{ source(
        'bsc_silver',
        'abis'
    ) }}