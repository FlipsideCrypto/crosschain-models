{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, start_date)",
    incremental_strategy = 'delete+insert',
) }}

SELECT
    DISTINCT 'optimism' AS blockchain,
    'flipside' AS creator,
    contract_address AS address,
    'erc-4626' AS tag_name,
    'token standard' AS tag_type,
    MIN(
        block_timestamp :: DATE
    ) AS start_date,
    NULL AS end_date,
    CURRENT_TIMESTAMP AS tag_created_at,
    MIN(_inserted_timestamp) AS _inserted_timestamp
FROM
    {{ source(
        'optimism_silver',
        'logs'
    ) }}
WHERE
    topics [0] IN (
        '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7',
        --4626 Deposit
        '0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db' -- 4626 Withdraw
    )

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP > (
    SELECT
        MAX(_INSERTED_TIMESTAMP)
    FROM
        {{ this }}
)
AND contract_address NOT IN (
    SELECT
        DISTINCT address
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    contract_address
HAVING
    COUNT(DISTINCT(topics [0])) = 2
