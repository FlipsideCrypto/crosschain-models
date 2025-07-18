{{ config(
    materialized = 'view',
    tags = ['metrics_daily']
) }}

SELECT
    blockchain,
    A.address AS token_address,
    A.symbol,
    CASE
        WHEN b.token_address IS NOT NULL THEN 'USD'
        WHEN C.tracks_asset IS NOT NULL THEN C.tracks_asset
        ELSE 'alt-token'
    END AS tracks_asset
FROM
    {{ ref('silver__tokens_enhanced') }} A
    LEFT JOIN (
        SELECT
            DISTINCT token_address
        FROM
            {{ ref('silver__tokens_stablecoins') }}
    ) b
    ON A.address = b.token_address
    LEFT JOIN (
        SELECT
            DISTINCT token_address,
            tracks_asset
        FROM
            {{ ref('silver__tokens_majors') }}
    ) C
    ON A.address = C.token_address
WHERE
    LOWER(
        A.blockchain
    ) = 'ethereum'
    AND A.is_verified
