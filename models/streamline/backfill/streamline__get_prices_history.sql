{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_get_coin_gecko_prices(object_construct('sql_source', '{{this.identifier}}','external_table', 'ASSET_PRICES_API', 'sql_limit', {{var('sql_limit','1000')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_history']
) }}

WITH runtimes AS (
    SELECT
        run_time
    FROM
        {{ ref("streamline__runtimes") }}
    EXCEPT
    SELECT 
        run_time
    FROM
        {{ ref("streamline__complete_get_prices_history") }}
    GROUP BY 
        run_time, 
        uid
    HAVING 
        COUNT(*) = 1
),
coins AS (
    SELECT
        id as coin_id
    FROM
        {{ source(
            'bronze_streamline',
            'asset_metadata_coin_gecko_api'
        ) }}
    WHERE
        _inserted_date = (
            SELECT
                MAX(_inserted_date)
            FROM
                {{ source(
                    'bronze_streamline',
                    'asset_metadata_coin_gecko_api'
                ) }}
            WHERE
                provider = 'coingecko'
        )
)
SELECT
    coins.coin_id AS id,
    run_time
FROM
    runtimes
JOIN
    coins
ON
    1=1
ORDER BY
    run_time ASC
