WITH defillama AS (
    SELECT
        LOWER(
            REGEXP_SUBSTR(REGEXP_REPLACE(TRIM(protocol), '-v\\d+', ''), '^[^ ]+')) AS platform_name,
            chain AS blockchain,
            SUM(volume) AS defillama_volume
            FROM
                {{ source(
                    'external_defillama',
                    'fact_dex_volume'
                ) }}
            WHERE
                DATE BETWEEN CURRENT_DATE() - 8
                AND CURRENT_DATE() - 1
            GROUP BY
                ALL
        ),
        crosschain AS (
            SELECT
                LOWER(
                    REGEXP_SUBSTR(REGEXP_REPLACE(TRIM(platform), '-v\\d+', ''), '^[^ ]+')) AS platform_name,
                    blockchain,
                    SUM(COALESCE(amount_in_usd, amount_out_usd)) AS crosschain_volume
                    FROM
                        {{ ref('defi__ez_dex_swaps') }}
                    WHERE
                        block_timestamp :: DATE BETWEEN CURRENT_DATE() - 8
                        AND CURRENT_DATE() - 1
                    GROUP BY
                        ALL
                ),
                crosschain_total_volume AS (
                    SELECT
                        blockchain,
                        SUM(crosschain_volume) AS total_crosschain_volume
                    FROM
                        crosschain
                    GROUP BY
                        ALL
                ),
                defillama_total_volume AS (
                    SELECT
                        blockchain,
                        SUM(defillama_volume) AS total_defillama_volume
                    FROM
                        defillama
                    GROUP BY
                        ALL
                ),
                comparison AS (
                    SELECT
                        COALESCE(
                            d.blockchain,
                            C.blockchain
                        ) AS blockchain,
                        COALESCE(
                            d.platform_name,
                            C.platform_name
                        ) AS platform_name,
                        defillama_volume,
                        ROUND(
                            defillama_volume / total_defillama_volume * 100,
                            4
                        ) AS defillama_vol_share,
                        crosschain_volume,
                        ROUND(
                            crosschain_volume / total_crosschain_volume * 100,
                            4
                        ) AS crosschain_vol_share
                    FROM
                        defillama d full
                        OUTER JOIN crosschain C USING (
                            blockchain,
                            platform_name
                        )
                        JOIN crosschain_total_volume USING (blockchain)
                        JOIN defillama_total_volume USING (blockchain)
                    ORDER BY
                        defillama_volume DESC nulls last
                ),
                fuzzy_match AS (
                    SELECT
                        blockchain,
                        d.platform_name AS platform_name
                    FROM
                        crosschain C
                        JOIN defillama d USING (blockchain)
                    WHERE
                        jarowinkler_similarity (
                            C.platform_name,
                            d.platform_name
                        ) > 85
                )
            SELECT
                blockchain,
                platform_name,
                defillama_volume,
                defillama_vol_share
            FROM
                comparison C
                LEFT JOIN fuzzy_match f USING (
                    blockchain,
                    platform_name
                )
            WHERE
                crosschain_volume IS NULL
                AND defillama_vol_share IS NOT NULL
                AND f.platform_name IS NULL qualify ROW_NUMBER() over (
                    PARTITION BY blockchain
                    ORDER BY
                        defillama_volume DESC nulls last
                ) <= 3
{%- endset %}

{% set test_results = run_query(query) %}

-- Store results
CREATE OR REPLACE TABLE crosschain.silver.dex_comparison_results AS
SELECT * FROM ({{ test_results }});
-- Store results
-- Output results in logs
{% if execute %}
    {% do log('TEST RESULTS:', info=True) %}
    {% do log(test_results.print_table(), info=True) %}
-- Output results in logs
{% if execute %}
-- Return original query for test evaluation
{{ query }}


    {% do log(test_results.print_table(), info=True) %}
{% endif %}

-- Return original query for test evaluation
{{ query }}


