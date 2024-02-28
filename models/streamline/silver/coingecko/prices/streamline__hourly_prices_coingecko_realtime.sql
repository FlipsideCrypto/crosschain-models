{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'ASSET_OHLC_API/COINGECKO', 'sql_limit', {{var('sql_limit','50000')}}, 'producer_batch_size', {{var('producer_batch_size','50000')}}, 'worker_batch_size', {{var('worker_batch_size','25000')}}, 'sm_secret_name','prod/coingecko/rest'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_prices_realtime2']
) }}

WITH assets AS (

    SELECT
        DISTINCT id AS asset_id
    FROM
        {{ ref("silver__asset_metadata_coin_gecko2") }}
    WHERE _inserted_timestamp = (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ ref("silver__asset_metadata_coin_gecko2") }}
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
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
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