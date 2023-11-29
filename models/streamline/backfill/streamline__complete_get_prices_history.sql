{{ config (
    materialized = "incremental",
    unique_key = "run_time",
    cluster_by = "run_time",
    merge_update_columns = [ "run_time"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(run_time)",
    tags = ['streamline_history']
) }}

SELECT
    id,
    run_time,
    metadata,
    data,
    error
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
 
qualify(ROW_NUMBER() over (PARTITION BY run_time
ORDER BY
    run_time DESC)) = 1
