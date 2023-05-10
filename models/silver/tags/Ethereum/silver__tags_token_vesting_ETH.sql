{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'merge',
    merge_update_columns = ['creator'],
) }}

WITH base_table AS (

    SELECT
        CASE
            WHEN event_name = 'VestClaimed' THEN decoded_flat :beneficiary
            WHEN event_name = 'VestingClaimed' THEN decoded_flat :investor
            WHEN event_name = 'VestedTokenRedeemed' THEN decoded_flat :"_to"
            WHEN event_name = 'VestingMemberAdded' THEN origin_from_address
            WHEN event_name IN (
                'VestingTransfer',
                'VestTransfer'
            ) THEN decoded_flat :to
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_flat :recipient IS NOT NULL THEN decoded_flat :recipient
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_flat :recipient IS NULL
            AND decoded_flat :"_address" IS NOT NULL THEN decoded_flat :"_address"
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_flat :recipient IS NULL
            AND decoded_flat :"_address" IS NULL
            AND decoded_flat :address IS NOT NULL THEN decoded_flat :address
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_flat :recipient IS NULL
            AND decoded_flat :"_address" IS NULL
            AND decoded_flat :address IS NULL
            AND decoded_flat :"_recipient" IS NOT NULL THEN decoded_flat :"_recipient"
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_flat :recipient IS NULL
            AND decoded_flat :"_address" IS NULL
            AND decoded_flat :address IS NULL
            AND decoded_flat :"_recipient" IS NULL
            AND decoded_flat :to IS NOT NULL THEN decoded_flat :to
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_flat :recipient IS NULL
            AND decoded_flat :"_address" IS NULL
            AND decoded_flat :address IS NULL
            AND decoded_flat :"_recipient" IS NULL
            AND decoded_flat :to IS NULL
            AND decoded_flat :ethereumAddress IS NOT NULL THEN decoded_flat :ethereumAddress
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_flat :recipient IS NULL
            AND decoded_flat :"_address" IS NULL
            AND decoded_flat :address IS NULL
            AND decoded_flat :"_recipient" IS NULL
            AND decoded_flat :to IS NULL
            AND decoded_flat :ethereumAddress IS NULL
            AND decoded_flat :account IS NOT NULL THEN decoded_flat :account
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_flat :recipient IS NULL
            AND decoded_flat :"_address" IS NULL
            AND decoded_flat :address IS NULL
            AND decoded_flat :"_recipient" IS NULL
            AND decoded_flat :to IS NULL
            AND decoded_flat :ethereumAddress IS NULL
            AND decoded_flat :account IS NULL 
            and decoded_flat :delegate is not null THEN decoded_flat :delegate
            ELSE decoded_flat :"_to"
        END AS wallets,
        MIN(_INSERTED_TIMESTAMP) AS _INSERTED_TIMESTAMP,
        MIN(
            block_timestamp :: DATE
        ) AS start_date
    FROM
        {{ source(
            'ethereum_silver',
            'decoded_logs_full'
        ) }}
    WHERE
        event_name IN (
            'Unlocked',
            'Vesting',
            'VestedRewardClaimed',
            'VestedAmountClaimed',
            'VestClaimed',
            'VestingClaimed',
            'VestingTransfer',
            'VestTransfer',
            'VestedTokenRedeemed',
            'VestingMemberAdded'
        )

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1
)
SELECT
    DISTINCT 'ethereum' AS blockchain,
    'flipside' AS creator,
    wallets AS address,
    'vested or locked token recipient' AS tag_name,
    'wallet' AS tag_type,
    start_date,
    NULL AS end_date,
    CURRENT_TIMESTAMP AS tag_created_at,
    _inserted_timestamp
FROM
    base_table
WHERE
    wallets IS NOT NULL
