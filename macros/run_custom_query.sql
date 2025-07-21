{% macro run_custom_query(query) %}
    {% do run_query(query) %}
{% endmacro %}
