{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_get_coin_gecko_prices(object_construct('sql_source', '{{this.identifier}}','external_table', 'ASSET_PRICES_API','producer_batch_size', '225000', 'worker_batch_size', '25000', 'sql_limit', '840000'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_prices_history']
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
        {{ ref("streamline__complete_coingecko_prices") }}
)
SELECT
    run_time,
    id
FROM
    runtimes
ORDER BY
    run_time ASC