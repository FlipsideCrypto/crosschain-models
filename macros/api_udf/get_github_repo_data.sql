{% macro get_github_repo_data() %}

{% set temp_table = target.database ~ ".silver.temp_github_repos" %}

{% set create_repos_temp %}
CREATE TABLE IF NOT EXISTS  {{temp_table}} AS 
SELECT repo_url,
    0 AS is_queried
FROM {{ target.database }}.silver.near_github_repos
LIMIT 20;
{% endset %}

{% do run_query(create_repos_temp) %}

{% set get_batch %}
SELECT 
    repo_url
FROM {{temp_table}}
WHERE is_queried = 0 
LIMIT 1;
{% endset %}

{% do log( get_batch ,"warn") %}  

{% if execute %}
{% set repos_list = run_query(get_batch).columns[0].values()|list %}
{% endif %}


{% if repos_list|length > 0 %}    
    {% set sql %}
    CALL {{ target.database }}.bronze_api.get_github_repo_data('last_year', {{repos_list}} )
    {% endset %}
    
    {% set repos_query_result = run_query(sql) %}
    
    {% do log( repos_query_result ,"warn") %}  
    {% set response = repos_query_result.columns[0].values()[0] %}

    {% do log( response  ,"warn") %}  
    {% do log( 'completed' ,"warn") %}  
    {% do log( response == 'completed' ,"warn") %}  
    {% do log( response == 0 ,"warn") %} 

    {% set update_list = repos_list if response == "completed" else repos_list[:response|int] %}
    
    {% do log( "--------","warn") %}  

    {% do log( update_list ,"warn") %}  


    {% do log( "--------","warn") %}  
    
    {% do log( update_list ,"warn") %}  
    {% set set_repos_temp %}
        UPDATE {{temp_table}}
        SET is_queried = 1
        WHERE repo_url IN ('{{ update_list|join("', '") }}');
    {% endset %}

    {% do log( set_repos_temp ,"warn") %}    
    {% do run_query(set_repos_temp) %}

    {% do log( update_list ,"warn") %}
    {% do log( repos_list ,"warn") %}

    {% if update_list !=  repos_list%}
        {% do log( "Sleeping" ,"warn") %}
        {% do run_query("SELECT SYSTEM$WAIT(3600);") %}
    {% endif %}

    {# Also do something for the TOKEN in github, and move endpoint to all weekly #}

        {% do log( "Recursive call" ,"warn") %}
        {{ get_github_repo_data() }}

{% endif %}
  {% set delete_temp_table %}
    DROP TABLE IF EXISTS {{temp_table}};
    {% endset %}
    {% do run_query(delete_temp_table) %}
    
    {% do log( "Deleting temp" ,"warn") %}
{% endmacro %}