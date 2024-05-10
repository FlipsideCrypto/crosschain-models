{{ config(
    materialized = 'incremental',
    unique_key = 'nft_address',
    full_refresh = false,
    tags = ['daily']
) }}

SELECT
    '0x4fdf87d4edae3fe323b8f6df502ccac6c8b4ba28' AS nft_address,
    'ethereum' :: VARCHAR AS blockchain,
    'interval_dev' :: VARCHAR AS discord_user,
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
