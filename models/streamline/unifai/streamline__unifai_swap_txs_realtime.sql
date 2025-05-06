{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"UNIFAI_SWAP_TXS",
        "sql_limit" :"100",
        "producer_batch_size" :"100",
        "worker_batch_size" :"25",
        "sql_source": "{{this.identifier}}",
        "exploded_key": tojson(["data"]),
        }
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
    crosschain.live.udf_api(
        'GET',
        CONCAT(
            '{service}swap-txns?startDate=',
            TO_VARCHAR(run_time, 'YYYY-MM-DD'), 'T00%3A00%3A00Z&endDate=',
            TO_VARCHAR(run_time, 'YYYY-MM-DD'), 'T23%3A59%3A59Z'
        ),
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'Authorization', '{authentication}'
        ),
        {},
        'Vault/prod/crosschain/unifai'
    ) AS request
from run_times
