{{ config (
    materialized = "incremental",
    unique_key = "uid",
    cluster_by = "run_time::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(uid)",
    tags = ['streamline_prices_complete']
) }}

SELECT
    id,
    run_time,
    metadata,
    data,
    error,
    id || '-' || run_time::VARCHAR as uid,
    metadata$file_last_modified as _inserted_at
FROM

{{ source( "bronze_streamline", "asset_prices_coin_gecko_api") }}

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

