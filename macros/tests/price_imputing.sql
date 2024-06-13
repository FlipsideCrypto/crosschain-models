{% test price_imputing(
    model,
    column_name
) %}
WITH base_test AS (
    SELECT
        token_address,
        blockchain,
        COUNT(
            {{ column_name }}
        ) AS num_imputed
    FROM
        {{ model }}
    WHERE
        is_imputed = TRUE
        AND HOUR >= SYSDATE() - INTERVAL '90 days'
    GROUP BY
        1,
        2
    HAVING
        num_imputed >= ((90 * 24) - 1) --flag if imputing for 89 days
)
SELECT
    COUNT(
        DISTINCT token_address
    ) AS num_address
FROM
    base_test
HAVING
    num_address > 0 
{% endtest %}

--if test fails, run below query to check if adhoc price history job is required, then run adhoc job cmd: 
--dbt run --vars '{"STREAMLINE_INVOKE_STREAMS":True, "ASSET_ID":"<insert_id>"}' -m "crosschain_models,tag:streamline_cg_prices_history"
{# WITH calls AS (
    SELECT
        'hunny-love-token' AS id,
        --pass unique asset_id if necessary
        CONCAT(
            '{service}/api/v3/coins/',
            id,
            '/market_chart?vs_currency=usd&days=90&interval=hourly&precision=full&x_cg_pro_api_key={Authentication}'
        ) AS api_url -- FROM
        --     run_times
    GROUP BY
        1
),
resp AS (
    SELECT
        DATE_PART(
            'EPOCH',
            DATEADD('day', -91, SYSDATE()) :: DATE) AS partition_key,
            crosschain.live.udf_api(
                'GET',
                api_url,
                NULL,
                NULL,
                'vault/prod/coingecko/rest'
            ) AS request
            FROM
                calls
        )
    SELECT
        *,
        TO_TIMESTAMP(
            VALUE [0] :: STRING
        ) AS recorded_ts,
        VALUE [1] :: STRING AS price
    FROM
        resp,
        LATERAL FLATTEN(
            input => request :data :prices
        )
    ORDER BY
        recorded_ts DESC; #}
