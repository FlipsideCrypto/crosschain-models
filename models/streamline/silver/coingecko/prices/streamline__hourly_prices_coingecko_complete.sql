{{ config (
    materialized = "incremental",
    unique_key = "uid",
    cluster_by = "run_time::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(uid)",
    tags = ['streamline_prices_complete2']
) }}

SELECT
    id,
    run_time,
    metadata,
    DATA,
    error,
    id || '-' || run_time :: VARCHAR AS UID,
    metadata$file_last_modified AS _inserted_at
FROM
    {{ source(
        "bronze_streamline",
        "asset_prices_coin_gecko_api"
    ) }} --update to use `bronze__streamline_hourly_prices_coingecko_history` table
WHERE
    TRUE

{% if is_incremental() %}
AND _inserted_at >= COALESCE(
    (
        SELECT
            MAX(_inserted_at) _inserted_at
        FROM
            {{ this }}
    ),
    '1900-01-01' :: timestamp_ntz
)
{% endif %}
--union in `bronze__streamline_hourly_prices_coingecko_realtime` table