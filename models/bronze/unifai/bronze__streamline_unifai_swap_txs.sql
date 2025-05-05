{{ config (
    materialized = 'view'
) }}

{% set model = "unifai_swap_txs" %}
{{ streamline_external_table_query_v2(
    model,
    partition_function = "CAST(SPLIT_PART(file_name, '/', 3) AS STRING )"
) }}