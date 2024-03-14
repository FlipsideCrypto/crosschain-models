{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'ASSET_MARKET_CHART_API/COINGECKO', 'sql_limit', {{var('sql_limit','2000')}}, 'producer_batch_size', {{var('producer_batch_size','2000')}}, 'worker_batch_size', {{var('worker_batch_size','2000')}}, 'sm_secret_name','prod/coingecko/rest'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_cg_prices_history']
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
        run_time
    FROM
        assets
        CROSS JOIN {{ ref('streamline__runtimes_daily') }}
    WHERE
        run_time >= DATEADD('day', -91, SYSDATE())
        AND run_time <= DATEADD('day', -1, SYSDATE())
    EXCEPT
    SELECT
        id AS asset_id,
        recorded_date AS run_time
    FROM
        {{ ref('streamline__hourly_prices_coingecko_complete') }}
    WHERE
        run_time >= DATEADD('day', -91, SYSDATE())
        AND run_time <= DATEADD('day', -1, SYSDATE())
),
calls AS (
    SELECT
        DISTINCT asset_id,
        '{service}/api/v3/coins/' || asset_id || '/market_chart?vs_currency=usd&days=90&interval=hourly&precision=full&x_cg_pro_api_key={Authentication}' AS api_url
    FROM
        run_times
)
SELECT
    DATE_PART(
        'EPOCH',
        DATEADD('day', -91, SYSDATE()) :: DATE) AS partition_key,
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
