{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
) }}

SELECT
    DISTINCT 'ethereum' AS blockchain,
    'flipside' AS creator,
    decoded_flat :instantiation :: STRING AS address,
    'gnosis safe address' AS tag_name,
    'contract' AS tag_type,
    DATE_TRUNC(
        'day',
        block_timestamp
    ) AS start_date,
    NULL AS end_date,
    _inserted_timestamp,
    CURRENT_TIMESTAMP AS tag_created_at
FROM
    {{ source(
        'ethereum_silver',
        'decoded_logs'
    ) }}
WHERE
    event_name = 'ContractInstantiation'

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY address
ORDER BY
    start_date ASC)) = 1
