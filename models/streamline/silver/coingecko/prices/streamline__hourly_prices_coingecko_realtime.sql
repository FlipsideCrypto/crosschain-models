{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'ASSET_OHLC_API/COINGECKO', 'sql_limit', {{var('sql_limit','10')}}, 'producer_batch_size', {{var('producer_batch_size','10')}}, 'worker_batch_size', {{var('worker_batch_size','10')}}, 'sm_secret_name','prod/coingecko/rest'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_prices_realtime']
) }}

WITH calls AS (

    SELECT
        '{service}/api/v3/coins/' || asset_id || '/ohlc?vs_currency=usd&days=1&x_cg_pro_api_key={Authentication}' calls,
        asset_id
    FROM
        (
            SELECT
                id as asset_id
            FROM
                {{ ref("bronze__streamline_asset_metadata_coingecko") }}
            WHERE
                _inserted_date = (
                    SELECT
                        MAX(_inserted_date)
                    FROM
                        {{ ref("bronze__streamline_asset_metadata_coingecko") }}
                )
        )
)
SELECT
    ARRAY_CONSTRUCT(
        DATE_PART('EPOCH', CURRENT_DATE())::INTEGER,
        ARRAY_CONSTRUCT(
            'GET',
            calls,
            PARSE_JSON('{}'),
            PARSE_JSON('{}'),
            ''
        ) 
    )AS request
FROM
    calls
    