    {# Check for rate limit in the API and execute the corresponding query #}
    {% set check_rate_limit %}
    -- Add your rate limit check SQL here.
    {% endset %}
    {% do run_query(check_rate_limit) %}

    {# Update the temporary table to set queried repositories as 'queried' #}
    {% set set_repos_temp %}
        UPDATE temp_github_repos
        SET is_queried = 1
        WHERE repo_url IN ('{{ repos_list|join("', '") }}');
    {% endset %}
    {% do run_query(set_repos_temp) %}

    {# Recursive call to continue fetching more repository data #}
    {% do get_github_repo_data() %}