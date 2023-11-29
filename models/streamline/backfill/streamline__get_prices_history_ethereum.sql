{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_get_coin_gecko_prices(object_construct('sql_source', '{{this.identifier}}','external_table', 'ASSET_PRICES_API', 'sql_limit', {{var('sql_limit','5000')}}))",
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
    WHERE
        id = 'ethereum'
)
SELECT
    'ethereum' AS id,
    run_time
FROM
    runtimes
ORDER BY
    run_time ASC
