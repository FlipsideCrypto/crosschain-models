{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, start_date)",
    incremental_strategy = 'delete+insert',
) }}

SELECT
    DISTINCT 'avalanche' AS blockchain,
    'flipside' AS creator,
    decoded_flat: account :: STRING AS address,
    'erc-6551' AS tag_name,
    'token standard' AS tag_type,
    MIN(
        block_timestamp :: DATE
    ) AS start_date,
    NULL AS end_date,
    CURRENT_TIMESTAMP AS tag_created_at,
    MIN(_inserted_timestamp) AS _inserted_timestamp
FROM
    {{ source(
        'avalanche_silver',
        'decoded_logs'
    ) }}
WHERE
    contract_address = LOWER('0x02101dfB77FDE026414827Fdc604ddAF224F0921')
    AND event_name = 'AccountCreated'
    AND tx_status = 'SUCCESS'

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
    3
