{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
) }}

WITH nft_buys AS (

    SELECT
        DISTINCT buyer_address AS address,
        MAX(block_timestamp) AS block_timestamp,
        COUNT(
            DISTINCT tx_hash
        ) AS num_transactions
    FROM
        ethereum.core.ez_nft_sales
    GROUP BY
        1
),
nft_sells AS (
    SELECT
        DISTINCT seller_address AS address,
        MAX(block_timestamp) AS block_timestamp,
        COUNT(
            DISTINCT tx_hash
        ) AS num_transactions
    FROM
        ethereum.core.ez_nft_sales
    GROUP BY
        1
),
nft_receipts AS (
    SELECT
        DISTINCT nft_to_address AS address,
        MAX(block_timestamp) AS block_timestamp,
        COUNT(
            DISTINCT tx_hash
        ) AS num_transactions
    FROM
        ethereum.core.ez_nft_transfers
    GROUP BY
        1
),
nft_transfers AS (
    SELECT
        DISTINCT nft_from_address AS address,
        MAX(block_timestamp) AS block_timestamp,
        COUNT(
            DISTINCT tx_hash
        ) AS num_transactions
    FROM
        ethereum.core.ez_nft_transfers
    GROUP BY
        1
),
total_transactions AS (
    SELECT
        *
    FROM
        nft_buys
    UNION
    SELECT
        *
    FROM
        nft_sells
    UNION
    SELECT
        *
    FROM
        nft_receipts
    UNION
    SELECT
        *
    FROM
        nft_transfers
),
total_transactions_small AS (
    SELECT
        DISTINCT address,
        SUM(num_transactions) AS total_transactions,
        MAX(block_timestamp) AS start_date,
        NTILE(100) over(
            ORDER BY
                total_transactions
        ) AS transaction_group
    FROM
        total_transactions
    GROUP BY
        1
),
nft_top_1_new AS (
    SELECT
        'ethereum' AS blockchain,
        'flipside' AS creator,
        address,
        CASE
            WHEN transaction_group = '100' THEN 'nft transactor top 1%'
            ELSE NULL
        END AS tag_name,
        'nft' AS tag_type,
        start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        total_transactions_small

{% if is_incremental() %}
LEFT OUTER JOIN (
    SELECT
        *
    FROM
        {{ this }}
    WHERE
        tag_name = 'nft transactor top 1%'
) b
ON A.user_address = b.address
{% endif %}
WHERE
    tag_name IS NOT NULL
),
nft_top_5_new AS (
    SELECT
        'ethereum' AS blockchain,
        'flipside' AS creator,
        address,
        CASE
            WHEN transaction_group IN (
                '100',
                '99',
                '98',
                '97',
                '96'
            ) THEN 'nft transactor top 5%'
            ELSE NULL
        END AS tag_name,
        'nft' AS tag_type,
        start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        total_transactions_small

{% if is_incremental() %}
LEFT OUTER JOIN (
    SELECT
        *
    FROM
        {{ this }}
    WHERE
        tag_name = 'nft transactor top 5%'
) b
ON A.user_address = b.address
{% endif %}
WHERE
    tag_name IS NOT NULL
),
nft_top_10_new AS (
    SELECT
        'ethereum' AS blockchain,
        'flipside' AS creator,
        address,
        CASE
            WHEN transaction_group IN (
                '100',
                '99',
                '98',
                '97',
                '96',
                '95',
                '94',
                '93',
                '92',
                '91'
            ) THEN 'nft transactor top 10%'
            ELSE NULL
        END AS tag_name,
        'nft' AS tag_type,
        start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        total_transactions_small

{% if is_incremental() %}
LEFT OUTER JOIN (
    SELECT
        *
    FROM
        {{ this }}
    WHERE
        tag_name = 'nft transactor top 10%'
) b
ON A.user_address = b.address
{% endif %}
WHERE
    tag_name IS NOT NULL
)

{% if is_incremental() %},
nft_top_1_cap AS (
    SELECT
        'ethereum' AS blockchain,
        'flipside' AS creator,
        address,
        'nft transactor top 1%' AS tag_name,
        'nft' AS tag_type,
        start_date,
        DATE_TRUNC(
            'DAY',
            CURRENT_DATE
        ) :: DATE AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        (
            SELECT
                *
            FROM
                {{ this }}
            WHERE
                tag_name = 'nft transactor top 1%'
        )
    WHERE
        address NOT IN (
            SELECT
                DISTINCT user_address
            FROM
                current_totals
            WHERE
                wallet_flag = 'nft transactor top 1%'
        )
),
nft_top_5_cap AS (
    SELECT
    SELECT
        'ethereum' AS blockchain,
        'flipside' AS creator,
        address,
        'nft transactor top 5%' AS tag_name,
        'nft' AS tag_type,
        start_date,
        DATE_TRUNC(
            'DAY',
            CURRENT_DATE
        ) :: DATE AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        (
            SELECT
                *
            FROM
                {{ this }}
            WHERE
                tag_name = 'nft transactor top 5%'
        )
    WHERE
        address NOT IN (
            SELECT
                DISTINCT user_address
            FROM
                current_totals
            WHERE
                wallet_flag = 'nft transactor top 5%'
        )
),
nft_top_10_new AS (
    SELECT
        'ethereum' AS blockchain,
        'flipside' AS creator,
        address,
        'nft transactor top 10%' AS tag_name,
        'nft' AS tag_type,
        start_date,
        DATE_TRUNC(
            'DAY',
            CURRENT_DATE
        ) :: DATE AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        (
            SELECT
                *
            FROM
                {{ this }}
            WHERE
                tag_name = 'nft transactor top 10%'
        )
    WHERE
        address NOT IN (
            SELECT
                DISTINCT user_address
            FROM
                current_totals
            WHERE
                wallet_flag = 'nft transactor top 10%'
        )
)
{% endif %}
SELECT
    *
FROM
    nft_top_1_new
UNION
SELECT
    *
FROM
    nft_top_5_new
UNION
SELECT
    *
FROM
    nft_top_10_new

{% if is_incremental() %}
UNION
SELECT
    *
FROM
    nft_top_1_cap
UNION
SELECT
    *
FROM
    nft_top_5_cap
UNION
SELECT
    *
FROM
    nft_top_10_cap
{% endif %}
