{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
) }}

SELECT
    DISTINCT 'bsc' AS blockchain,
    'flipside' AS creator,
    to_address :: STRING AS address,
    'contract address' AS tag_name,
    'contract' AS tag_type,
    block_number,
    DATE_TRUNC(
        'day',
        block_timestamp
    ) AS start_date,
    NULL AS end_date,
    _inserted_timestamp,
    CURRENT_TIMESTAMP AS tag_created_at
FROM
    {{ source(
        'bsc_silver',
        'traces'
    ) }}
WHERE
    TYPE in ('CREATE', 'CREATE2')
    AND tx_status = 'SUCCESS'
    AND to_address IS NOT NULL

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
    block_number DESC)) = 1
