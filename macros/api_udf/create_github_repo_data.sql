{% macro create_github_repo_data() %}
  {% set create_table %}
  CREATE SCHEMA IF NOT EXISTS {{ target.database }}.bronze_api;

  CREATE TABLE IF NOT EXISTS {{ target.database }}.bronze_api.github_repo_data(
      repo_owner STRING,
      repo_name STRING,
      endpoint_name STRING,
      data VARIANT,
      provider STRING,
      _inserted_timestamp TIMESTAMP_NTZ,
      _res_id STRING
  );
  {% endset %}
  {% do run_query(create_table) %}
  
  {% set query %}
  CREATE OR REPLACE PROCEDURE {{ target.database }}.bronze_api.get_github_repo_data() RETURNS VARIANT LANGUAGE SQL AS $$
  BEGIN
    let repos := ARRAY_CONSTRUCT('CADigitalNexus/Wikis');  -- Replace with your repository specifications
    let endpoints := ARRAY_CONSTRUCT(
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
    );
    let repo_counter := 0;
    let endpoint_counter := 0;
    
    let current_repo_full := '';
    let current_repo_owner := '';
    let current_repo_name := '';
    let current_endpoint := '';
    let endpoint_url := '';

    FOR repo_counter := 1 TO ARRAY_SIZE(repos) DO
        current_repo_full := repos[repo_counter];
        current_repo_owner := SPLIT_PART(current_repo_full, '/', 1);
        current_repo_name := SPLIT_PART(current_repo_full, '/', 2);
        
        FOR endpoint_counter = 1 TO ARRAY_SIZE(endpoints) DO
            current_endpoint := endpoints[endpoint_counter];
            endpoint_url := REPLACE(REPLACE(current_endpoint, '{owner}', current_repo_owner), '{repo}', current_repo_name); 
            
            -- Fetch and flatten the data
            CREATE OR REPLACE TEMPORARY TABLE response_data AS 
            WITH api_call AS (
                SELECT 
                    ethereum.streamline.udf_api('GET', endpoint_url, {}, { "Authorization": "Bearer YOUR_GITHUB_TOKEN" }) AS res,
                    CURRENT_TIMESTAMP AS _request_timestamp
            ),
            flatten_res AS (
                SELECT 
                    VALUE AS data,
                    'github' AS provider,
                    _request_timestamp AS _inserted_timestamp,
                    concat_ws('-', DATE_PART(epoch_second, _request_timestamp), current_repo_name, current_endpoint) AS _res_id
                FROM api_call
                -- Adjust the LATERAL FLATTEN part if needed, based on the structure of the returned data
            )

            INSERT INTO {{ target.database }}.bronze_api.github_repo_data(
                repo_owner,
                repo_name,
                endpoint_name,
                data,
                provider,
                _inserted_timestamp,
                _res_id
            )
            SELECT
                current_repo_owner,
                current_repo_name,
                current_endpoint,
                data,
                provider,
                _inserted_timestamp,
                _res_id
            FROM flatten_res;

        END FOR;
    END FOR;

    RETURN 'Data fetched for all endpoints and repos';
  END;$$ 
  {% endset %}
  {% do run_query(query) %}
{% endmacro %}
