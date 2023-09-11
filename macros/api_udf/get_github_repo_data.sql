{% macro get_github_repo_data() %}
{% set sql %}
CALL {{ target.database }}.bronze_api.get_github_repo_data()
{% endset %}
{% do run_query(sql) %}
{% endmacro %}