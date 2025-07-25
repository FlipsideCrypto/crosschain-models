{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'MARKET_DATA_API/COINGECKO', 'sql_limit', {{var('sql_limit','200')}}, 'producer_batch_size', {{var('producer_batch_size','5')}}, 'worker_batch_size', {{var('worker_batch_size','5')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_cg_market_data']
) }}

WITH page_numbers AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY _id) AS page_num
    FROM {{ ref('silver__number_sequence') }}
    WHERE _id <= {{ var('MAX_PAGES', 5) }} --update to 250 for prod
),
calls AS (
    SELECT
        page_num,
        '{service}/api/v3/coins/markets?vs_currency=usd&per_page=250&page=' || page_num || '&precision=full' AS api_url,
        {'x-cg-pro-api-key': '{Authentication}'} AS headers
    FROM page_numbers
)
SELECT
    page_num,
    DATE_PART('EPOCH', SYSDATE()::DATE) :: INTEGER AS partition_key,
    live.udf_api(
        'GET',
        api_url,
        headers,
        NULL,
        'vault/prod/coingecko/rest'
    ) AS request
FROM
    calls
