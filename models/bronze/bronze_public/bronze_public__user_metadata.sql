{{ config(
    materialized = 'incremental',
    unique_key = 'collection_address',
    full_refresh = false
) }}

SELECT
    '0x123' AS collection_address,
    'ethereum' AS blockchain,
    SYSDATE() AS _inserted_timestamp,
    'interval_dev' AS discord_user

{% if is_incremental() %}
FROM
    (
        SELECT
            1
    ) AS dummy
WHERE
    1 = 0
{% endif %}
