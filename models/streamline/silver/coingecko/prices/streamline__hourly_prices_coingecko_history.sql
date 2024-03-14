{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'ASSET_MARKET_CHART_API/COINGECKO', 'sql_limit', {{var('sql_limit','1000')}}, 'producer_batch_size', {{var('producer_batch_size','1000')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}, 'sm_secret_name','prod/coingecko/rest'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_cg_prices_history']
) }}

WITH assets AS (

    SELECT
        id AS asset_id
    FROM
        {{ ref('bronze__streamline_asset_metadata_coingecko') }}
    WHERE
        _inserted_timestamp = (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ ref('bronze__streamline_asset_metadata_coingecko') }}
        )
    --active tokens only
),
run_times AS (
    SELECT
        asset_id
    FROM assets
    EXCEPT
    SELECT
        id AS asset_id
    FROM
        {{ ref('streamline__hourly_prices_coingecko_complete') }}
    WHERE
        recorded_date >= DATEADD('day', -91, SYSDATE())
        AND recorded_date <= DATEADD('day', -1, SYSDATE())
        --replays 90 days of prices from active tokens missing all prices within the last 90 days
),
calls AS (
    SELECT
        {% if var("ASSET_ID", false) %}
            '{{ var("ASSET_ID") }}' AS id
        {% else %}
            asset_id AS id
        {% endif %}, --pass unique asset_id if necessary
        CONCAT(
                '{service}/api/v3/coins/',
                id,
                '/market_chart?vs_currency=usd&days=90&interval=hourly&precision=full&x_cg_pro_api_key={Authentication}'
            ) AS api_url
    FROM
        run_times
    GROUP BY 1
)
SELECT
    DATE_PART(
        'EPOCH',
        DATEADD('day', -91, SYSDATE()) :: DATE) AS partition_key,
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
ORDER BY
    partition_key ASC