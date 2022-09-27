{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, tag_name)",
    incremental_strategy = 'merge',
    merge_update_columns = ['creator'],
) }}

WITH base_table AS (

    SELECT
        CASE
            WHEN event_name = 'VestClaimed' THEN event_inputs :beneficiary
            WHEN event_name = 'VestingClaimed' THEN event_inputs :"_beneficiary"
            WHEN event_name = 'VestedTokenRedeemed' THEN event_inputs :"_to"
            WHEN event_name = 'VestingMemberAdded' THEN origin_from_address
            WHEN event_name IN (
                'VestingTransfer',
                'VestTransfer'
            ) THEN event_inputs :to
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND event_inputs :recipient IS NOT NULL THEN event_inputs :recipient
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND event_inputs :recipient IS NULL
            AND event_inputs :"_address" IS NOT NULL THEN event_inputs :"_address"
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND event_inputs :recipient IS NULL
            AND event_inputs :"_address" IS NULL
            AND event_inputs :address IS NOT NULL THEN event_inputs :address
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND event_inputs :recipient IS NULL
            AND event_inputs :"_address" IS NULL
            AND event_inputs :address IS NULL
            AND event_inputs :"_recipient" IS NOT NULL THEN event_inputs :"_recipient"
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND event_inputs :recipient IS NULL
            AND event_inputs :"_address" IS NULL
            AND event_inputs :address IS NULL
            AND event_inputs :"_recipient" IS NULL
            AND event_inputs :to IS NOT NULL THEN event_inputs :to
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND event_inputs :recipient IS NULL
            AND event_inputs :"_address" IS NULL
            AND event_inputs :address IS NULL
            AND event_inputs :"_recipient" IS NULL
            AND event_inputs :to IS NULL
            AND event_inputs :ethereumAddress IS NOT NULL THEN event_inputs :ethereumAddress
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND event_inputs :recipient IS NULL
            AND event_inputs :"_address" IS NULL
            AND event_inputs :address IS NULL
            AND event_inputs :"_recipient" IS NULL
            AND event_inputs :to IS NULL
            AND event_inputs :ethereumAddress IS NULL
            AND event_inputs :account IS NOT NULL THEN event_inputs :account
            ELSE event_inputs :"_to"
        END AS wallets,
        MIN(_INSERTED_TIMESTAMP) AS _INSERTED_TIMESTAMP,
        MIN(
            block_timestamp :: DATE
        ) AS start_date
    FROM
        {{ source(
            'ethereum_silver',
            'logs'
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
