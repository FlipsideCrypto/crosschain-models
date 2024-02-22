{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'ASSET_MARKET_CHART_API/COINGECKO', 'sql_limit', {{var('sql_limit','10')}}, 'producer_batch_size', {{var('producer_batch_size','10')}}, 'worker_batch_size', {{var('worker_batch_size','10')}}, 'sm_secret_name','prod/coingecko/rest'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_prices_history2']
) }}

WITH assets AS (

    SELECT
        DISTINCT id AS asset_id
    FROM
        {{ ref('silver__asset_metadata_coin_gecko2') }}
),
run_times AS (
    SELECT
        asset_id,
        run_time
    FROM
        assets
        CROSS JOIN {{ ref('streamline__runtimes2') }}
    WHERE
        run_time < DATEADD('day', -1, SYSDATE())
    EXCEPT
    SELECT
        id AS asset_id,
        run_time
    FROM
        {{ ref('streamline__hourly_prices_coingecko_complete') }}
    WHERE
        run_time < DATEADD('day', -1, SYSDATE())
    ),
calls AS (
    SELECT
        asset_id,
        DATE_PART(
            'EPOCH',
            run_time
        ) :: INTEGER AS run_time_epoch,
        '{service}/api/v3/coins/' || asset_id || '/market_chart/range?vs_currency=usd&from=' || run_time_epoch || '&to=' || run_time_epoch || '&x_cg_pro_api_key={Authentication}' AS api_url
    FROM
        run_times
    )
SELECT
    run_time_epoch AS partition_key,
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
