{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'ASSET_OHLC_API/COINMARKETCAP', 'sql_limit', {{var('sql_limit','50000')}}, 'producer_batch_size', {{var('producer_batch_size','50000')}}, 'worker_batch_size', {{var('worker_batch_size','25000')}}, 'sm_secret_name','prod/prices/coinmarketcap'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_cmc_prices_history']
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
        run_time :: DATE >= '2024-03-08' --set to model deployment date to avoid extended backfill
        AND
        run_time < DATEADD('hour', -2, SYSDATE())
    EXCEPT
    SELECT
        id AS asset_id,
        run_time
    FROM
        {{ ref('streamline__hourly_prices_coinmarketcap_complete') }}
    WHERE
        run_time :: DATE >= '2024-03-08' --set to model deployment date to avoid extended backfill
        AND
        run_time < DATEADD('hour', -2, SYSDATE())
),
numbered_assets AS (
    SELECT
        asset_id,
        run_time,
        FLOOR((ROW_NUMBER() OVER (PARTITION BY run_time ORDER BY asset_id::INT) - 1) / 100) AS group_id
    FROM
        run_times
),
grouped_assets AS (
    SELECT
        group_id,
        run_time,
        LISTAGG(asset_id, ',') WITHIN GROUP (ORDER BY asset_id::INT) AS ids
    FROM
        numbered_assets
    GROUP BY
        1,2
),
calls AS (
    SELECT
        group_id,
        DATE_PART(
            'EPOCH',DATEADD('HOUR', -1, run_time)
        ) :: INTEGER AS start_time_epoch,
        DATE_PART(
            'EPOCH',date_trunc('hour',run_time)
            ) :: INTEGER AS end_time_epoch,
        '{service}/v2/cryptocurrency/ohlcv/historical?interval=hourly&time_period=hourly&time_start=' || start_time_epoch || '&time_end=' || end_time_epoch || '&id=' || ids AS api_url
    FROM
        grouped_assets
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
LIMIT 200
--CMC API SAFE REQUEST LIMIT