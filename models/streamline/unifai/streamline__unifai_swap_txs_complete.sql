{{ config (
    materialized = "incremental",
    unique_key = ['partition_key'],
    cluster_by = "partition_key"
) }}

SELECT
    partition_key,
    TO_TIMESTAMP_NTZ(partition_key, 'YYYY_MM_DD') as run_date,
    _inserted_timestamp
FROM
    {{ ref('bronze__streamline_unifai_swap_txs') }}

{% if is_incremental() %}
where _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}


