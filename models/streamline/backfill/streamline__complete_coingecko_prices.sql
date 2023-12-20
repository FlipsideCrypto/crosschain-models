{{ config (
    materialized = "incremental",
    unique_key = "uid",
    cluster_by = "run_time::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(uid)",
    tags = ['streamline_prices','streamline_coin_gecko_api','streamline_backfill']
) }}

SELECT
    id,
    run_time,
    metadata,
    data,
    error,
    id || '-' || run_time::VARCHAR as uid
FROM

{{ source( "bronze_streamline", "asset_prices_coin_gecko_api") }}

WHERE
    TRUE
{% if is_incremental() %}
    AND run_time >= COALESCE(
        (
            SELECT
                MAX(run_time) run_time
            FROM
                {{ this }}
        ),
        '1900-01-01' :: timestamp_ntz
    )
{% endif %}
 
qualify(ROW_NUMBER() over (PARTITION BY uid
ORDER BY
    run_time DESC)) = 1
