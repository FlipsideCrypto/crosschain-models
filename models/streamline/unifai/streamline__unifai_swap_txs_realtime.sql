{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'UNIFAI_SWAP_TXS', 'sql_limit', {{var('sql_limit','100')}}, 'producer_batch_size', {{var('producer_batch_size','100')}}, 'worker_batch_size', {{var('worker_batch_size','25')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_unifai_swap_txs_realtime']
) }}

WITH run_times AS (
    SELECT
        run_time
    FROM {{ ref('streamline__runtimes_daily') }}
    where run_time >= '2025-03-28'
    
    EXCEPT

    SELECT
        run_date as run_time
    FROM {{ ref('streamline__unifai_swap_txs_complete') }}
)

SELECT
    TO_CHAR(TO_TIMESTAMP_NTZ(run_time), 'YYYY_MM_DD') AS partition_key,
    live.udf_api(
        'GET',
        CONCAT(
            'https://uniq-data-api.unifai.network/swap-txns?startDate=',
            TO_VARCHAR(run_time, 'YYYY-MM-DD'), 'T00%3A00%3A00Z&endDate=',
            TO_VARCHAR(run_time, 'YYYY-MM-DD'), 'T23%3A59%3A59Z'
        ),
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'Authorization', 'w5mu060ZvFgPsutC1Zj91GnaR3zWlujl'
        ),
        {}
    ) AS request
from run_times
