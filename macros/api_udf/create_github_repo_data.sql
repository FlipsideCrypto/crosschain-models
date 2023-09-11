{% macro create_github_repo_data() %}
{% set create_table %}
CREATE schema if NOT EXISTS {{ target.database }}.bronze_api;

CREATE TABLE if NOT EXISTS {{ target.database }}.bronze_api.github_repo_data(
    repo_name STRING,
    endpoint_name STRING,
    data VARIANT,
    _inserted_timestamp TIMESTAMP_NTZ,
    _res_id STRING
);
{% endset %}
{% do run_query(create_table) %}
{% set query %}
CREATE OR REPLACE PROCEDURE {{ target.database }}.bronze_api.get_github_repo_data() returns VARIANT LANGUAGE SQL AS $$
BEGIN
    -- Replace with the list of repositories you are interested in
    let repos = ARRAY['owner/repo1', 'owner/repo2', ...];  
    let endpoints = ARRAY[
        '/repos/{owner}/{repo}/community/profile',
        '/repos/{owner}/{repo}/stats/code_frequency',
        '/repos/{owner}/{repo}/stats/commit_activity',
        '/repos/{owner}/{repo}/stats/contributors',
        '/repos/{owner}/{repo}/stats/participation',
        '/repos/{owner}/{repo}/stats/punch_card',
        '/repos/{owner}/{repo}/pulls',
        '/repos/{owner}/{repo}/issues',
        '/repos/{owner}/{repo}/stargazers',
        '/repos/{owner}/{repo}/subscribers',
        '/repos/{owner}/{repo}/commits',
        '/repos/{owner}/{repo}/releases',
        '/repos/{owner}/{repo}/forks'
    ];
    let base_url = 'https://api.github.com';

    FOR repo in repos
    BEGIN
        FOR endpoint in endpoints
        BEGIN
            -- Replace {owner} and {repo} in endpoint with actual values
            let endpoint_url = base_url || REPLACE(REPLACE(endpoint, '{owner}', SPLIT_PART(repo, '/', 1)), '{repo}', SPLIT_PART(repo, '/', 2));
            let response = livequery.live.udf_api('GET', endpoint_url, { "Authorization": "Bearer YOUR_GITHUB_TOKEN" });  -- Replace YOUR_GITHUB_TOKEN with your actual token
            -- Insert the result into the bronze_api.github_repo_data table
            INSERT INTO {{ target.database }}.bronze_api.github_repo_data(
                repo_name,
                endpoint_name,
                data,
                _inserted_timestamp,
                _res_id
            )
            VALUES (
                repo,
                endpoint,
                response,
                CURRENT_TIMESTAMP,
                repo || '-' || endpoint
            );
        END;
    END;
    RETURN 'Success';
END;$$ 
{% endset %}
{% do run_query(query) %}
{% endmacro %}
