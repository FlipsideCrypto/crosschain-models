{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, tag_name, start_date)",
    incremental_strategy = 'delete+insert',
) }}
-- We do not want to full refresh this model until we have a historical tags code set up.
-- to full-refresh either include the variable allow_full_refresh: True to command or comment out below code
-- DO NOT FORMAT will break the full refresh code if formatted copy from below

-- {% if execute %}
--   {% if flags.FULL_REFRESH and var('allow_full_refresh', False) != True %}
--       {{ exceptions.raise_compiler_error("Full refresh is not allowed for this model unless the argument \"- -vars 'allow_full_refresh: True'\" is included in the dbt run command.") }}
--   {% endif %}
-- {% endif %}
{% if execute %}
  {% if flags.FULL_REFRESH and var('allow_full_refresh', False) != True %}
      {{ exceptions.raise_compiler_error("Full refresh is not allowed for this model unless the argument \"- -vars 'allow_full_refresh: True'\" is included in the dbt run command.") }}
  {% endif %}
{% endif %}

WITH nft_receipts AS (
    SELECT
        DISTINCT nft_to_address AS address,
        MAX(block_timestamp) AS block_timestamp,
        COUNT(
            DISTINCT tx_hash
        ) AS num_transactions
    FROM
        {{ source(
            'ethereum_nft',
            'ez_nft_transfers'
        ) }}
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
        {{ source(
            'ethereum_nft',
            'ez_nft_transfers'
        ) }}
    GROUP BY
        1
),
total_transactions AS (
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
        A.address,
        CASE
            WHEN A.transaction_group = '100' THEN 'nft transactor top 1%'
            ELSE NULL
        END AS tag_name,
        'nft' AS tag_type,
        A.start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        total_transactions_small A
    WHERE
        A.transaction_group = '100'

{% if is_incremental() %}
AND A.address NOT IN (
    SELECT
        DISTINCT address
    FROM
        {{ this }}
    WHERE
        tag_name = 'nft transactor top 1%'
)
{% endif %}
),
nft_top_5_new AS (
    SELECT
        'ethereum' AS blockchain,
        'flipside' AS creator,
        A.address,
        CASE
            WHEN A.transaction_group IN (
                '100',
                '99',
                '98',
                '97',
                '96'
            ) THEN 'nft transactor top 5%'
            ELSE NULL
        END AS tag_name,
        'nft' AS tag_type,
        A.start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        total_transactions_small A
    WHERE
        A.transaction_group IN (
            '100',
            '99',
            '98',
            '97',
            '96'
        )

{% if is_incremental() %}
AND A.address NOT IN (
    SELECT
        DISTINCT address
    FROM
        {{ this }}
    WHERE
        tag_name = 'nft transactor top 5%'
)
{% endif %}
),
nft_top_10_new AS (
    SELECT
        'ethereum' AS blockchain,
        'flipside' AS creator,
        A.address,
        CASE
            WHEN A.transaction_group IN (
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
        A.start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        total_transactions_small A
    WHERE
        A.transaction_group IN (
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
        )

{% if is_incremental() %}
AND A.address NOT IN (
    SELECT
        DISTINCT address
    FROM
        {{ this }}
    WHERE
        tag_name = 'nft transactor top 10%'
)
{% endif %}
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
                DISTINCT address
            FROM
                total_transactions_small
            WHERE
                transaction_group = '100'
        )
),
nft_top_5_cap AS (
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
                DISTINCT address
            FROM
                total_transactions_small
            WHERE
                transaction_group IN (
                    '100',
                    '99',
                    '98',
                    '97',
                    '96'
                )
        )
),
nft_top_10_cap AS (
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
                DISTINCT address
            FROM
                total_transactions_small
            WHERE
                transaction_group IN (
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
                )
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
