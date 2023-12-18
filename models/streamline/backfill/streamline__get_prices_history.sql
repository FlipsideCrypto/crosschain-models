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
        run_time,
        id
    FROM
        {{ ref("streamline__runtimes") }}
        JOIN {{ ref("bronze__asset_metadata_coin_gecko") }}
        ON 1 = 1
    WHERE
        _inserted_date = (
            SELECT
                MAX(_inserted_date)
            FROM
                {{ ref("bronze__asset_metadata_coin_gecko") }}
        )
    EXCEPT
    SELECT
        run_time,
        id
    FROM
        {{ ref("streamline__complete_get_prices_history") }}
)
SELECT
    run_time,
    id
FROM
    runtimes
ORDER BY
    run_time ASC
