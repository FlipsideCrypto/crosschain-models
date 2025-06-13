WITH base AS (
    SELECT
        blockchain,
        LOWER(address) AS address
    FROM
        {{ ref('silver__tokens_enhanced') }}
    WHERE
        is_verified
        AND COALESCE(
            coingecko_id,
            coinmarketcap_id
        ) IS NULL
    EXCEPT
    SELECT
        blockchain,
        LOWER(address) AS address
    FROM
        {{ ref('silver__manual_verified_token_mapping') }}
)
SELECT
    blockchain,
    address,
    blockchain || ',' || address || ',,,,' AS copy_into_seed_value
FROM
    base
ORDER BY
    1,
    2
