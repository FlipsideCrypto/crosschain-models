{{ config(
    materialized = 'view',
) }}

SELECT
    'coingecko' AS provider,
    id,
    token_address,
    NAME,
    symbol,
    platform
FROM
    {{ ref('silver__asset_metadata_coin_gecko') }}
