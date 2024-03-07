{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'ASSET_OHLC_API/COINMARKETCAP', 'sql_limit', {{var('sql_limit','50000')}}, 'producer_batch_size', {{var('producer_batch_size','50000')}}, 'worker_batch_size', {{var('worker_batch_size','25000')}}, 'sm_secret_name','prod/prices/coinmarketcap'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_prices_realtime2']
) }}

WITH assets AS (

    SELECT
        DISTINCT id AS asset_id
    FROM
        {{ ref("bronze__streamline_asset_metadata_coinmarketcap") }}
    WHERE
        _inserted_timestamp = (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ ref("bronze__streamline_asset_metadata_coinmarketcap") }}
        )
),
run_times AS (
    SELECT
        asset_id,
        run_time
    FROM
        assets A
        CROSS JOIN {{ ref("streamline__runtimes_hourly") }}
    WHERE
        run_time > DATEADD('hour', -2, SYSDATE())
    EXCEPT
    SELECT
        id AS asset_id,
        run_time
    FROM
        {{ ref('streamline__hourly_prices_coinmarketcap_complete') }}
    WHERE
        run_time > DATEADD('hour', -2, SYSDATE())
),
calls AS (
    SELECT
        asset_id,
        DATE_PART(
            'EPOCH',
            run_time
        ) :: INTEGER AS start_time_epoch,
        DATE_PART(
            'EPOCH',
            DATEADD(
                'hour',
                1,
                run_time
            )
        ) :: INTEGER AS end_time_epoch,
        '{service}/v2/cryptocurrency/ohlcv/historical?interval=hourly&time_period=hourly&time_start=' || start_time_epoch || '&time_end=' || end_time_epoch || '&id=' || asset_id AS api_url
    FROM
        run_times
)
SELECT
    end_time_epoch AS partition_key,
    ARRAY_CONSTRUCT(
        partition_key,
        ARRAY_CONSTRUCT(
            'GET',
            api_url,
            PARSE_JSON('{"Accept": "application/json", "Accept-Encoding": "deflate, gzip", "X-CMC_PRO_API_KEY": "{Authentication}"}'),
            PARSE_JSON('{}'),
            ''
        )
    ) AS request
FROM
    calls
ORDER BY
    partition_key ASC
