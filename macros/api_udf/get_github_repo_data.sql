{% macro get_github_repo_data(frequency, GITHUB_TOKEN) %}

{% set table_name = target.database ~ ".silver.github_repos" %}

{% set create_repos_endpoints_table %}
CREATE TABLE IF NOT EXISTS {{ table_name }} AS (
    SELECT 
        'near' AS project_name,
        repo_url, 
        frequency, 
        CAST(NULL AS TIMESTAMP_NTZ) AS last_time_queried,
        REPLACE(
            REPLACE(endpoint, '{owner}', SPLIT_PART(repo_url, '/', 1)), 
            '{repo}', SPLIT_PART(repo_url, '/', 2)
        ) AS full_endpoint,
        CASE 
            WHEN ARRAY_SIZE(SPLIT(full_endpoint, '/')) = 6 THEN SPLIT_PART(full_endpoint, '/', 6)
            WHEN ARRAY_SIZE(SPLIT(full_endpoint, '/')) = 5 THEN SPLIT_PART(full_endpoint, '/', 5)
            ELSE 'general'
        END as endpoint_github
    FROM {{ target.database }}.silver.near_github_repos
    CROSS JOIN (
        SELECT 'daily' AS frequency, endpoint FROM VALUES
            ('/repos/{owner}/{repo}/pulls'),
            ('/repos/{owner}/{repo}/issues'),
            ('/repos/{owner}/{repo}/stargazers'),
            ('/repos/{owner}/{repo}/subscribers'),
            ('/repos/{owner}/{repo}/commits'),
            ('/repos/{owner}/{repo}/releases'),
            ('/repos/{owner}/{repo}/forks')
        AS t(endpoint)
        
        UNION ALL
        
        SELECT 'weekly' AS frequency, endpoint FROM VALUES
            ('/repos/{owner}/{repo}/stats/code_frequency'),
            ('/repos/{owner}/{repo}/stats/contributors'),
            ('/repos/{owner}/{repo}/stats/participation'),
            ('/repos/{owner}/{repo}')
        AS t(endpoint)
        
        UNION ALL
        
        SELECT 'last_year' AS frequency, endpoint FROM VALUES
            ('/repos/{owner}/{repo}/stats/commit_activity')
        AS t(endpoint)
    ) AS endpoints
);

{% endset %}

{% do run_query(create_repos_endpoints_table) %}

{% set frequency_string = frequency | join("','") %}

{% set check_endpoints_query %}
    SELECT COUNT(*)
    FROM {{ table_name }}
    WHERE (DATE(last_time_queried) <> SYSDATE()::DATE OR last_time_queried IS NULL)
    AND frequency IN ('{{ frequency_string }}')
{% endset %}

{% set results = run_query(check_endpoints_query) %}

{% set first_row = results.rows[0] %}
{% set count_value = first_row[0] %}

{% if count_value > 0 %}
    {% set sql %}
    CALL {{ target.database }}.bronze_api.get_github_api_repo_data( {{frequency}}, '{{ GITHUB_TOKEN }}')
    {% endset %}
    
    {% do run_query(sql) %}
{% endif %}

{% endmacro %}