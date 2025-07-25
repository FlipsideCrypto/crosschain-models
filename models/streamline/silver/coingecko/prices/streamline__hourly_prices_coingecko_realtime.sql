{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'ASSET_OHLC_API/COINGECKO', 'sql_limit', {{var('sql_limit','50000')}}, 'producer_batch_size', {{var('producer_batch_size','50000')}}, 'worker_batch_size', {{var('worker_batch_size','25000')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_cg_prices_realtime']
) }}

WITH assets AS (

    SELECT
        DISTINCT id AS asset_id
    FROM
        {{ ref("bronze__streamline_asset_metadata_coingecko") }}
    WHERE
        _inserted_timestamp = (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ ref("bronze__streamline_asset_metadata_coingecko") }}
        )
),
calls AS (
    SELECT
        asset_id,
        '{service}/api/v3/coins/' || asset_id || '/ohlc?vs_currency=usd&days=1&x_cg_pro_api_key={Authentication}' AS api_url
    FROM
        assets
)
SELECT
    DATE_PART('EPOCH', SYSDATE()::DATE) :: INTEGER AS partition_key,
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