{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', blockchain, symbol, xfer_date)",
    incremental_strategy = 'delete+insert',
) }}

WITH core_values AS (

    SELECT
        A.blockchain,
        A.symbol,
        b.xfer_date
    FROM
        (
            SELECT
                DISTINCT blockchain,
                symbol
            FROM
                {{ ref('silver__ntr') }}
            WHERE
                blockchain != 'algoana'
            ORDER BY
                blockchain,
                symbol
        ) A
        CROSS JOIN (
            SELECT
                DISTINCT DATE_TRUNC(
                    'day',
                    HOUR
                ) AS xfer_date
            FROM
                {{ source(
                    'bronze',
                    'legacy_hours'
                ) }}
            WHERE
                xfer_date >= (
                    SELECT
                        MIN(xfer_date)
                    FROM
                        {{ ref('silver__ntr') }}
                )
                AND xfer_date <= CURRENT_DATE
        ) b
    ORDER BY
        A.blockchain,
        A.symbol,
        b.xfer_date
),
base AS (
    SELECT
        DISTINCT blockchain,
        symbol,
        xfer_date,
        SUM(reward) AS reward,
        SUM(hodl) AS hodl,
        SUM(unlabeled_transfer) AS unlabeled_transfer,
        SUM(stake) AS stake,
        SUM(cex_deposit) AS cex_deposit,
        SUM(nft_buy) AS nft_buy,
        SUM(dex_swap) AS dex_swap,
        SUM(bridge) AS bridge,
        SUM(
            CASE
                WHEN first_is_bounty = 'TRUE' THEN 1
                ELSE 0
            END
        ) / COUNT(*) AS prop_first_is_bounty,
        SUM(
            CASE
                WHEN did_hunt = 'TRUE' THEN 1
                ELSE 0
            END
        ) / COUNT(*) AS prop_did_hunt,
        SUM(
            CASE
                WHEN did_new_user = 'TRUE' THEN 1
                ELSE 0
            END
        ) / COUNT(*) AS prop_did_new_user,
        SUM(
            CASE
                WHEN did_bounty = 'TRUE' THEN 1
                ELSE 0
            END
        ) / COUNT(*) AS prop_did_bounty
    FROM
        {{ ref('silver__ntr') }}
    WHERE
        reward > 0
        AND blockchain != 'algoana'
    GROUP BY
        blockchain,
        symbol,
        xfer_date
    ORDER BY
        blockchain,
        symbol,
        xfer_date
),
joined_base AS (
    SELECT
        A.blockchain,
        A.symbol,
        A.xfer_date,
        b.reward,
        b.hodl,
        b.unlabeled_transfer,
        b.stake,
        b.cex_deposit,
        b.nft_buy,
        b.dex_swap,
        b.bridge,
        b.prop_first_is_bounty,
        b.prop_did_hunt,
        b.prop_did_new_user,
        b.prop_did_bounty
    FROM
        core_values A
        LEFT JOIN base b
        ON A.blockchain = b.blockchain
        AND A.symbol = b.symbol
        AND A.xfer_date = b.xfer_date
),
ff_base AS(
    SELECT
        *,
        COUNT(
            CASE
                WHEN reward IS NOT NULL THEN 1
            END
        ) over (
            PARTITION BY blockchain,
            symbol
            ORDER BY
                blockchain,
                symbol,
                xfer_Date rows BETWEEN unbounded preceding
                AND CURRENT ROW
        ) AS grp
    FROM
        joined_base
),
final_upload AS (
    SELECT
        blockchain,
        symbol,
        xfer_date,
        MIN(reward) over (
            PARTITION BY blockchain,
            symbol,
            grp
        ) AS reward,
        MIN(hodl) over (
            PARTITION BY blockchain,
            symbol,
            grp
        ) AS hodl,
        MIN(unlabeled_transfer) over (
            PARTITION BY blockchain,
            symbol,
            grp
        ) AS unlabeled_transfer,
        MIN(stake) over (
            PARTITION BY blockchain,
            symbol,
            grp
        ) AS stake,
        MIN(cex_deposit) over (
            PARTITION BY blockchain,
            symbol,
            grp
        ) AS cex_deposit,
        MIN(nft_buy) over (
            PARTITION BY blockchain,
            symbol,
            grp
        ) AS nft_buy,
        MIN(dex_swap) over (
            PARTITION BY blockchain,
            symbol,
            grp
        ) AS dex_swap,
        MIN(bridge) over (
            PARTITION BY blockchain,
            symbol,
            grp
        ) AS bridge,
        MIN(prop_first_is_bounty) over (
            PARTITION BY blockchain,
            symbol,
            grp
        ) AS prop_first_is_bounty,
        MIN(prop_did_hunt) over (
            PARTITION BY blockchain,
            symbol,
            grp
        ) AS prop_did_hunt,
        MIN(prop_did_new_user) over (
            PARTITION BY blockchain,
            symbol,
            grp
        ) AS prop_did_new_user,
        MIN(prop_did_bounty) over (
            PARTITION BY blockchain,
            symbol,
            grp
        ) AS prop_did_bounty
    FROM
        ff_base
    ORDER BY
        blockchain,
        symbol,
        xfer_date
)
SELECT
    blockchain,
    symbol,
    xfer_date,
    reward,
    hodl,
    unlabeled_transfer,
    stake,
    cex_deposit,
    nft_buy,
    dex_swap,
    bridge,
    prop_first_is_bounty,
    prop_did_hunt,
    prop_did_new_user,
    prop_did_bounty
FROM
    final_upload
WHERE
    reward IS NOT NULL
ORDER BY
    blockchain,
    symbol,
    xfer_date
