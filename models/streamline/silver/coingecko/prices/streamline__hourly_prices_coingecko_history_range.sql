{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'ASSET_OHLC_API/COINGECKO', 'sql_limit', {{var('sql_limit','50000')}}, 'producer_batch_size', {{var('producer_batch_size','50000')}}, 'worker_batch_size', {{var('worker_batch_size','25000')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_cg_prices_history_range']
) }}

WITH asset_ids AS (
    SELECT value::STRING AS id
    FROM TABLE(FLATTEN(INPUT => PARSE_JSON(
        {% if var('ASSET_IDS') is string %}
            '["{{ var("ASSET_IDS") }}"]'
        {% else %}
            '{{ var("ASSET_IDS") | tojson }}'
        {% endif %}
    ))) --pass unique asset_ids when necessary as a single string or array of strings, e.g. "ASSET_IDS":"wrapped-avax" or "ASSET_IDS":["wrapped-avax", "usd-coin"]
),
calls AS (
    SELECT
        id,
        '{{ var("START_DATE") }}' :: DATE AS start_date, --format must be string "YYYY-MM-DD", e.g. "START_DATE":"2025-02-01"
        '{{ var("END_DATE") }}' :: DATE AS end_date, --format must be string "YYYY-MM-DD", e.g. "END_DATE":"2025-02-28"
        DATE_PART('EPOCH', start_date) :: INTEGER AS start_ts,
        DATE_PART('EPOCH', end_date) :: INTEGER AS end_ts,
        '{service}/api/v3/coins/' || id || '/ohlc/range?vs_currency=usd&from=' || start_ts || '&to=' || end_ts || '&interval=hourly&precision=full&x_cg_pro_api_key={Authentication}' AS api_url
    FROM asset_ids
)
SELECT
    start_ts AS partition_key,
    live.udf_api(
        'GET',
        api_url,
        NULL,
        NULL,
        'vault/prod/coingecko/rest'
    ) AS request
FROM
    calls

--API Documentation:
--https://docs.coingecko.com/reference/coins-id-ohlc-range

--Data Availability:
-- Hourly Interval (interval=hourly):
-- up to 31 days per request/ 744 hourly interval candles.
-- Available from 9 February 2018 onwards (1518147224 epoch time).

-- Example Usage:
-- dbt run -m "crosschain_models,tag:streamline_cg_prices_history_range" --vars '{"STREAMLINE_INVOKE_STREAMS":true,"ASSET_IDS":["wrapped-avax", "usd-coin"],"START_DATE":"2025-02-01","END_DATE":"2025-02-28"}'