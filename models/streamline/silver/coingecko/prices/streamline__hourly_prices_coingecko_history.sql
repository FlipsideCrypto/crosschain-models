{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'ASSET_MARKET_CHART_API/COINGECKO', 'sql_limit', {{var('sql_limit','50000')}}, 'producer_batch_size', {{var('producer_batch_size','50000')}}, 'worker_batch_size', {{var('worker_batch_size','50000')}}, 'sm_secret_name','prod/coingecko/rest'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_prices_history2']
) }}

WITH assets AS (

    SELECT
        DISTINCT id AS asset_id
    FROM
        {{ ref('bronze__streamline_asset_metadata_coingecko') }}
    WHERE
        _inserted_timestamp = (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ ref('bronze__streamline_asset_metadata_coingecko') }}
        )
),
run_times AS (
    SELECT
        asset_id,
        run_time :: DATE AS run_time
    FROM
        assets A
        CROSS JOIN {{ ref('streamline__runtimes2') }}
    WHERE
        run_time :: DATE BETWEEN '2024-01-15' :: DATE
        AND DATEADD('day', -1, SYSDATE()) -- temp logic for backfill
        -- run_time :: DATE BETWEEN DATEADD('day',-{{ var("LOOKBACK", 45) }}, SYSDATE())
        -- AND DATEADD('day', -1, SYSDATE()) -- long term logic
    EXCEPT
    SELECT
        id AS asset_id,
        run_time :: DATE AS run_time
    FROM
        {{ ref('streamline__hourly_prices_coingecko_complete') }}
    WHERE
        run_time :: DATE < DATEADD('day', -1, SYSDATE())
        ),
        calls AS (
            SELECT
                asset_id,
                DATE_PART(
                    'EPOCH',
                    run_time :: DATE
                ) :: INTEGER AS start_time_epoch,
                DATE_PART(
                    'EPOCH',
                    DATEADD('day',1,run_time :: DATE)
                ) :: INTEGER AS end_time_epoch,
                '{service}/api/v3/coins/' || asset_id || '/market_chart/range?vs_currency=usd&from=' || start_time_epoch || '&to=' || end_time_epoch || '&x_cg_pro_api_key={Authentication}' AS api_url
            FROM
                run_times
        )
    SELECT
        start_time_epoch AS partition_key,
        ARRAY_CONSTRUCT(
            partition_key,
            ARRAY_CONSTRUCT(
                'GET',
                api_url,
                PARSE_JSON('{}'),
                PARSE_JSON('{}'),
                ''
            )
        ) AS request
    FROM
        calls
    ORDER BY
        partition_key ASC
    LIMIT
        100 --remove after testing
