{{ config(
    materialized = 'incremental',
    unique_key = 'nft_address',
    full_refresh = false
) }}

SELECT
    '0x0000000000000000000000000000000000000000' AS nft_address,
    'ethereum' AS blockchain,
    'interval_dev' AS discord_user,
    SYSDATE() AS _inserted_timestamp

{% if is_incremental() %}
FROM
    (
        SELECT
            1
    ) AS dummy
WHERE
    1 = 0
{% endif %}
