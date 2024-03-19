{{ config (
    materialized = "incremental",
    unique_key = ['id','run_time'],
    cluster_by = "run_time::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)",
    tags = ['streamline_cmc_prices_complete']
) }}

WITH base AS (

    SELECT
        id,
        DATE_TRUNC(
            'hour',
            f.value :quote :USD :timestamp :: TIMESTAMP
        ) AS run_time,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_hourly_prices_coinmarketcap') }}
        s,
        LATERAL FLATTEN(
            input => DATA :data :quotes
        ) f
    WHERE
        run_time IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    id,
    run_time,
    {{ dbt_utils.generate_surrogate_key(['id','run_time']) }} AS hourly_prices_coinmarketcap_complete_id,
    _inserted_timestamp
FROM
    base qualify(ROW_NUMBER() over (PARTITION BY id, run_time
ORDER BY
    _inserted_timestamp DESC)) = 1
