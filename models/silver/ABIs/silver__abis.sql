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
