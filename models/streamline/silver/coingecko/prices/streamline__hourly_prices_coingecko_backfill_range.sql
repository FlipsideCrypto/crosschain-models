{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'ASSET_MARKET_CHART_API/COINGECKO', 'sql_limit', {{var('sql_limit','1000')}}, 'producer_batch_size', {{var('producer_batch_size','1000')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
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
    --active tokens only
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
    --active tokens that are missing one day of prices within the last 90 days
),
numbered_assets AS (
    SELECT
        DISTINCT asset_id,
        DENSE_RANK() OVER (ORDER BY asset_id ASC) AS row_num
    FROM
        run_times
),
calls AS (
    SELECT
        asset_id,
        '{service}/api/v3/coins/' || asset_id || '/market_chart?vs_currency=usd&days=90&interval=hourly&precision=full&x_cg_pro_api_key={Authentication}' AS api_url
    FROM
        numbered_assets
    WHERE row_num >= {{ var("RANGE_START", 0) }} 
        AND row_num <= {{ var("RANGE_END", 1000) }}
)
SELECT
    DATE_PART(
        'EPOCH',
        DATEADD('day', -91, SYSDATE()) :: DATE) AS partition_key,
        {{ target.database }}.live.udf_api(
            'GET',
            api_url,
            NULL,
            NULL,
            'vault/prod/coingecko/rest'
        ) AS request
FROM
    calls
ORDER BY
    partition_key ASC