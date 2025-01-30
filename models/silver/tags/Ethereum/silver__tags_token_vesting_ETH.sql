{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'merge',
    merge_update_columns = ['creator', 'modified_timestamp'],
    tags = ['daily']
) }}

WITH base_table AS (

    SELECT
        CASE
            WHEN event_name = 'VestClaimed' THEN decoded_log :beneficiary
            WHEN event_name = 'VestingClaimed' THEN decoded_log :investor
            WHEN event_name = 'VestedTokenRedeemed' THEN decoded_log :"_to"
            WHEN event_name = 'VestingMemberAdded' THEN origin_from_address
            WHEN event_name IN (
                'VestingTransfer',
                'VestTransfer'
            ) THEN decoded_log :to
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_log :recipient IS NOT NULL THEN decoded_log :recipient
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_log :recipient IS NULL
            AND decoded_log :"_address" IS NOT NULL THEN decoded_log :"_address"
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_log :recipient IS NULL
            AND decoded_log :"_address" IS NULL
            AND decoded_log :address IS NOT NULL THEN decoded_log :address
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_log :recipient IS NULL
            AND decoded_log :"_address" IS NULL
            AND decoded_log :address IS NULL
            AND decoded_log :"_recipient" IS NOT NULL THEN decoded_log :"_recipient"
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_log :recipient IS NULL
            AND decoded_log :"_address" IS NULL
            AND decoded_log :address IS NULL
            AND decoded_log :"_recipient" IS NULL
            AND decoded_log :to IS NOT NULL THEN decoded_log :to
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_log :recipient IS NULL
            AND decoded_log :"_address" IS NULL
            AND decoded_log :address IS NULL
            AND decoded_log :"_recipient" IS NULL
            AND decoded_log :to IS NULL
            AND decoded_log :ethereumAddress IS NOT NULL THEN decoded_log :ethereumAddress
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_log :recipient IS NULL
            AND decoded_log :"_address" IS NULL
            AND decoded_log :address IS NULL
            AND decoded_log :"_recipient" IS NULL
            AND decoded_log :to IS NULL
            AND decoded_log :ethereumAddress IS NULL
            AND decoded_log :account IS NOT NULL THEN decoded_log :account
            WHEN event_name IN (
                'Unlocked',
                'Vesting',
                'VestedRewardClaimed',
                'VestedAmountClaimed'
            )
            AND decoded_log :recipient IS NULL
            AND decoded_log :"_address" IS NULL
            AND decoded_log :address IS NULL
            AND decoded_log :"_recipient" IS NULL
            AND decoded_log :to IS NULL
            AND decoded_log :ethereumAddress IS NULL
            AND decoded_log :account IS NULL 
            and decoded_log :delegate is not null THEN decoded_log :delegate
            ELSE decoded_log :"_to"
        END AS wallets,
        MIN(modified_timestamp) AS _inserted_timestamp,
        MIN(
            block_timestamp :: DATE
        ) AS start_date
    FROM
        {{ source(
            'ethereum_core',
            'ez_decoded_event_logs'
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
    _inserted_timestamp,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address']) }} AS tags_token_vesting_eth_id,
    '{{ invocation_id }}' as _invocation_id  
FROM
    base_table
WHERE
    wallets IS NOT NULL
