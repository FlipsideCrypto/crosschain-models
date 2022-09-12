{{ config(
    materialized = 'view',
) }}


SELECT
    'coingecko' AS provider,
    p.id,
    p.recorded_hour,
    p.open,
    p.high,
    p.low,
    p.close
FROM
    {{ ref('silver__hourly_prices_coin_gecko') }}
    p
