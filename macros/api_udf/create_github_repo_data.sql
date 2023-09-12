{% macro create_github_repo_data() %}
  {% set create_table %}
  CREATE SCHEMA IF NOT EXISTS {{ target.database }}.bronze_api;
  CREATE OR REPLACE TABLE {{ target.database }}.bronze_api.github_repo_data(
    repo_owner STRING,
    repo_name STRING,
    endpoint_name STRING,
    data VARIANT,
    provider STRING,
    frequency_type STRING,
    _inserted_timestamp TIMESTAMP_NTZ,
    _res_id STRING
  );
  {% endset %}
  {% do run_query(create_table) %}
  
  {% set query %}
  CREATE OR REPLACE PROCEDURE {{ target.database }}.bronze_api.get_github_repo_data("fetch_frequency" STRING) RETURNS VARIANT LANGUAGE javascript EXECUTE AS CALLER AS $$

    function logMessage(message) {
       let logSql = `SELECT SYSTEM$LOG('${message}')`;
       snowflake.execute({sqlText: logSql});
    }


    let repos = ['CADigitalNexus/Wikis'];  // Replace with your repository specifications
    
    let endpointMapping = {
        'daily': [
            '/repos/{owner}/{repo}/pulls',
            '/repos/{owner}/{repo}/issues',
            '/repos/{owner}/{repo}/stargazers',
            '/repos/{owner}/{repo}/subscribers',
            '/repos/{owner}/{repo}/commits',
            '/repos/{owner}/{repo}/releases',
            '/repos/{owner}/{repo}/forks'
        ],
        'weekly': [
            '/repos/{owner}/{repo}/stats/code_frequency',
            '/repos/{owner}/{repo}/stats/contributors',
            '/repos/{owner}/{repo}/stats/participation',
            '/repos/{owner}/{repo}/stats/punch_card'
        ],
        'last_year': [
            '/repos/{owner}/{repo}/stats/commit_activity'
        ]
    };

    let endpoints = endpointMapping[fetch_frequency];
    let current_repo_full = '';
    let current_repo_owner = '';
    let current_repo_name = '';
    let current_endpoint = '';
    let endpoint_url = '';
    let base_url = 'https://api.github.com'

    for (var repo_counter = 0; repo_counter < repos.length; repo_counter++) {
        current_repo_full = repos[repo_counter];
        var parts = current_repo_full.split('/');
        current_repo_owner = parts[0];
        current_repo_name = parts[1];

        for (var endpoint_counter = 0; endpoint_counter < endpoints.length; endpoint_counter++) {
            current_endpoint = endpoints[endpoint_counter];
            let segments = current_endpoint.split('/');
            let lastSegment = segments[segments.length - 1];
            endpoint_url = base_url + current_endpoint.replace('{owner}', current_repo_owner).replace('{repo}', current_repo_name);
            var create_temp_table_command = `
                CREATE OR REPLACE TEMPORARY TABLE response_data AS 
                WITH api_call AS (
                    SELECT 
                        ethereum.streamline.udf_api('GET', '${endpoint_url}', { 'Authorization': 'token ', 'Accept': 'application/vnd.github.v3+json' },{}) AS res,
                        CURRENT_TIMESTAMP AS _request_timestamp
                ),
                flatten_res AS (
                    SELECT 
                        res AS data,
                        'github' AS provider,
                        _request_timestamp AS _inserted_timestamp,
                        concat_ws('-', DATE_PART(epoch_second, _request_timestamp), '${current_repo_name}', '${endpoint_url}') AS _res_id
                    FROM api_call
                )
                SELECT
                    data,
                    provider,
                    _inserted_timestamp,
                    _res_id
                FROM
                flatten_res;
            `;

            snowflake.execute({sqlText: create_temp_table_command.replace('{endpoint_url}', endpoint_url).replace('{repo}', current_repo_name)});

            // Second command: Insert data into the target table from the temporary table
            var insert_command = `
                INSERT INTO {{ target.database }}.bronze_api.github_repo_data(
                    repo_owner,
                    repo_name,
                    endpoint_name,
                    data,
                    provider,
                    frequency_type,
                    _inserted_timestamp,
                    _res_id
                )
                SELECT
                    '${current_repo_owner}' as repo_owner,  
                    '${current_repo_name}' as repo_name,
                    '${lastSegment}' as endpoint_name,
                    data,
                    provider,
                    '${fetch_frequency}' AS frequency_type,
                    _inserted_timestamp,
                    _res_id
                FROM response_data;
            `;
            snowflake.execute({sqlText: insert_command});

        };
    };
    return 'Data fetched for all endpoints and repos';
  $$ 
  {% endset %}
  {% do run_query(query) %}
{% endmacro %}


