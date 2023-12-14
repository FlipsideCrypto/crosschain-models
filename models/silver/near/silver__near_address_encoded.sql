{{ config(
    materialized = 'incremental',
    unique_key = "near_address"
) }}

WITH log_address AS (

    SELECT
        DISTINCT receiver_id AS near_address,
        CONCAT('0x', SHA2(near_address, 256)) AS addr_encoded,
        _inserted_timestamp
    FROM
        {{ source(
            'near_silver',
            'logs_s3'
        ) }}

{% if is_incremental() %}
WHERE
    near_address NOT IN (
        SELECT
            DISTINCT near_address
        FROM
            {{ this }}
    )
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '24 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    DISTINCT signer_id AS near_address,
    CONCAT('0x', SHA2(near_address, 256)) AS addr_encoded,
    _inserted_timestamp
FROM
    {{ source(
        'near_silver',
        'logs_s3'
    ) }}

{% if is_incremental() %}
WHERE
    near_address NOT IN (
        SELECT
            DISTINCT near_address
        FROM
            {{ this }}
    )
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '24 hours'
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    near_address,
    addr_encoded,
    _inserted_timestamp
FROM
    log_address qualify(ROW_NUMBER() over (PARTITION BY near_address
ORDER BY
    _inserted_timestamp DESC)) = 1
