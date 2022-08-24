WITH base_table AS (
    SELECT
        block_timestamp :: DATE AS balance_date,
        CASE
            WHEN symbol = '' eth '' THEN '' eth ''
            ELSE contract_address
        END AS contract_address,
        user_address,
        symbol,
        current_bal
    FROM
        ethereum.core.ez_balance_diffs
    WHERE
        block_timestamp :: DATE >= '''' --user input
        AND user_address = '''' --user input
        AND (
            contract_address = '''' --user input
            OR symbol = '' eth ''
        )
),
all_days AS (
    SELECT
        date_day AS balance_date
    FROM
        ethereum.core.dim_dates
),
address_ranges AS (
    SELECT
        user_address,
        contract_address,
        symbol,
        MIN(
            balance_date :: DATE
        ) AS min_block_date,
        CURRENT_DATE() :: DATE AS max_block_date
    FROM
        base_table
    GROUP BY
        user_address,
        contract_address,
        symbol,
        max_block_date
),
all_dates AS (
    SELECT
        C.balance_date,
        A.user_address,
        A.contract_address,
        A.symbol
    FROM
        all_days C
        LEFT JOIN address_ranges A
        ON C.full_balance_date BETWEEN A.min_block_date
        AND A.max_block_date
    WHERE
        A.user_address IS NOT NULL
),
eth_balances AS (
    SELECT
        user_address,
        contract_address,
        balance_date,
        current_bal,
        TRUE AS daily_activity
    FROM
        base_table
),
balance_tmp AS (
    SELECT
        d.balance_date,
        d.user_address,
        d.contract_address,
        d.symbol,
        b.current_bal,
        b.daily_activity
    FROM
        all_dates d
        LEFT JOIN eth_balances b
        ON d.balance_date = b.balance_date
        AND d.user_address = b.user_address
        AND d.contract_address = b.contract_address
),
FINAL AS (
    SELECT
        balance_date,
        user_address,
        contract_address,
        symbol,
        LAST_VALUE(
            current_bal ignore nulls
        ) over(
            PARTITION BY user_address,
            contract_address
            ORDER BY
                balance_date ASC rows unbounded preceding
        ) AS balance,
        CASE
            WHEN daily_activity IS NULL THEN FALSE
            ELSE TRUE
        END AS daily_activity
    FROM
        balance_tmp
)
SELECT
    *
FROM
    FINAL
WHERE
    balance <> 0
ORDER BY
    balance_date DESC,
    contract_address
