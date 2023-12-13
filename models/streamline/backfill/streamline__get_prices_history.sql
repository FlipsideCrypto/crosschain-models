{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_grpc_us_east_2(object_construct('sql_source', '{{this.identifier}}','node_url','access-001.mainnet22.nodes.onflow.org:9000','external_table', 'blocks', 'sql_limit', {{var('sql_limit','500000')}}, 'producer_batch_size', {{var('producer_batch_size','10000')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_history']
) }}

WITH runtimes AS (
    SELECT
        run_time
    FROM
        {{ ref("streamline__runtimes") }}
    EXCEPT
    SELECT 
        run_time
    FROM
        {{ ref("streamline__complete_get_prices_history") }}
    GROUP BY 
        run_time, 
        uid
    HAVING 
        COUNT(*) = 1
),
coins AS (
    SELECT
        id as coin_id
    FROM
        {{ source(
            'bronze_streamline',
            'asset_metadata_coin_gecko_api'
        ) }}
    WHERE
        _inserted_date = (
            SELECT
                MAX(_inserted_date)
            FROM
                {{ source(
                    'bronze_streamline',
                    'asset_metadata_coin_gecko_api'
                ) }}
            WHERE
                provider = 'coingecko'
        )
)
SELECT
    coins.coin_id AS id,
    run_time
FROM
    runtimes
JOIN
    coins
ON
    1=1
ORDER BY
    run_time ASC
