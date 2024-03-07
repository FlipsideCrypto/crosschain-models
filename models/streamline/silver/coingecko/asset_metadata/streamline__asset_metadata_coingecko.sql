{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'ASSET_METADATA_API/COINGECKO', 'sql_limit', {{var('sql_limit','10')}}, 'producer_batch_size', {{var('producer_batch_size','10')}}, 'worker_batch_size', {{var('worker_batch_size','10')}}, 'sm_secret_name','prod/coingecko/rest'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_cg_asset_metadata']
) }}

WITH calls AS (

    SELECT
        '{service}/api/v3/coins/list?include_platform=true&x_cg_pro_api_key={Authentication}' AS api_url
)
SELECT
    DATE_PART('EPOCH', SYSDATE())::INTEGER AS partition_key,
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
    -- needs to run 10-15 min prior to prices workflows (asset metadata referenced in history + realtime models)