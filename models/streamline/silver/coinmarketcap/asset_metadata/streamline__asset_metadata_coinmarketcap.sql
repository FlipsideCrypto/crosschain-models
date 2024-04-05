{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'ASSET_METADATA_API/COINMARKETCAP', 'sql_limit', {{var('sql_limit','10')}}, 'producer_batch_size', {{var('producer_batch_size','10')}}, 'worker_batch_size', {{var('worker_batch_size','10')}}, 'sm_secret_name','prod/prices/coinmarketcap'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_cmc_asset_metadata']
) }}

WITH calls AS (

    SELECT
        '{service}/v1/cryptocurrency/map' AS api_url
)
SELECT
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        api_url,
        PARSE_JSON('{"Accept": "application/json", "Accept-Encoding": "deflate, gzip", "X-CMC_PRO_API_KEY": "{Authentication}"}'),
        {},
        'vault/prod/prices/coinmarketcap'
    ) AS request
FROM
    calls -- needs to run 10-15 min prior to prices workflows (asset metadata referenced in history + realtime models)
