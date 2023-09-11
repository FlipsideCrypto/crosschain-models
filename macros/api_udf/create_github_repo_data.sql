{% macro create_github_repo_data() %}
{% set create_table %}
CREATE SCHEMA IF NOT EXISTS {{ target.database }}.bronze_api;

CREATE TABLE IF NOT EXISTS {{ target.database }}.bronze_api.github_repo_data(
    repo_name STRING,
    endpoint_name STRING,
    data VARIANT,
    _inserted_timestamp TIMESTAMP_NTZ,
    _res_id STRING
);
{% endset %}
{% do run_query(create_table) %}
{% set query %}
CREATE OR REPLACE PROCEDURE {{ target.database }}.bronze_api.get_github_repo_data() RETURNS VARIANT LANGUAGE SQL AS $$
BEGIN
    let base_url := 'https://api.github.com';
    let repos := ARRAY_CONSTRUCT['CADigitalNexus/Wikis'];
    let endpoints := ARRAY_CONSTRUCT[
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
    let counter:= 0;
    let repo_counter:= 0;
    let endpoint_counter:= 0;
    let total_repos := ARRAY_SIZE(repos);
    let total_endpoints := ARRAY_SIZE(endpoints);

    REPEAT
        -- Extract current repo and endpoint
        let current_repo = repos[repo_counter + 1];
        let current_endpoint = endpoints[endpoint_counter + 1];
        let endpoint_url = base_url || REPLACE(REPLACE(current_endpoint, '{owner}', SPLIT_PART(current_repo, '/', 1)), '{repo}', SPLIT_PART(current_repo, '/', 2));
        let response = livequery.live.udf_api('GET', endpoint_url, { "Authorization": "Bearer YOUR_GITHUB_TOKEN" });

        -- Insert the result into the bronze_api.github_repo_data table
        INSERT INTO {{ target.database }}.bronze_api.github_repo_data(
            repo_name,
            endpoint_name,
            data,
            _inserted_timestamp,
            _res_id
        )
        VALUES (
            current_repo,
            current_endpoint,
            response,
            CURRENT_TIMESTAMP,
            current_repo || '-' || current_endpoint
        );

        -- Update counters
        endpoint_counter:= endpoint_counter + 1;
        IF endpoint_counter = total_endpoints THEN
            endpoint_counter := 0;
            repo_counter:= repo_counter + 1;
        END IF;
        counter:= counter + 1;

    UNTIL repo_counter = total_repos
    END REPEAT;

    RETURN 'Success';
END;$$ 
{% endset %}
{% do run_query(query) %}
{% endmacro %}
