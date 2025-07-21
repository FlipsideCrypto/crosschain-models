{% macro get_high_volume_protocols() %}
    {% set query %}
SELECT
    modified_timestamp :: DATE AS DATE,
    blockchain,
    platform_name AS protocol,
    defillama_volume_percent
FROM
    {{ ref('silver_metrics__bridge_comparison') }}
WHERE
    defillama_volume_percent > 9.99 
    qualify RANK() over (
        ORDER BY
            modified_timestamp :: DATE DESC
    ) = 1 
    {% endset %}
  {% set results = run_query(query) %}
    {% do log("Query executed successfully", info=true) %}
    {% do log("Number of rows returned: " ~ results.rows | length, info=true) %}
    
    {% for row in results.rows %}
        {% do log("Row: " ~ row, info=true) %}
    {% endfor %}
{% endmacro %}
