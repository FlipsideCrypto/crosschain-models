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
    AND asset_id :: STRING = '2396' --temp for testing
),
calls AS (
    SELECT
        asset_id,
        DATE_PART(
            'EPOCH',
            DATEADD('day', -1, SYSDATE())
        ) :: INTEGER AS start_time_epoch,
        DATE_PART(
            'EPOCH',
            SYSDATE()
        ) :: INTEGER AS end_time_epoch,
        '{service}/v2/cryptocurrency/ohlcv/historical?time_period=hourly&time_start=' || start_time_epoch || '&time_end=' || end_time_epoch || '&interval=hourly&id=' || asset_id AS api_url
    FROM
        assets
)
SELECT
    start_time_epoch AS partition_key,
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