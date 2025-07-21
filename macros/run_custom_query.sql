{% macro run_custom_query(query) %}
    {% set results = run_query(query) %}
    {% do log(
        "Query executed successfully",
        info = true
    ) %}
    {% do log(
        "Number of rows returned: " ~ results.rows | length,
        info = true
    ) %}
    {% for row in results.rows %}
        {% do log(
            "Row: " ~ row,
            info = true
        ) %}
    {% endfor %}
{% endmacro %}
