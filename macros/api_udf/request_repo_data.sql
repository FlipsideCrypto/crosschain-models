{% macro request_repo_data() %}

{% set create_table %}
CREATE SCHEMA IF NOT EXISTS {{ target.database }}.bronze_api;
CREATE TABLE IF NOT EXISTS {{ target.database }}.bronze_api.github_repo_data(
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
CREATE OR REPLACE PROCEDURE {{ target.database }}.bronze_api.geta_github_repo_data("fetch_frequency" ARRAY, "token" STRING) 
    RETURNS STRING 
    LANGUAGE JAVASCRIPT 
    EXECUTE AS CALLER 
    AS $$


    function logMessage(message) {
       let logSql = `SELECT SYSTEM$LOG('${message}')`;
       snowflake.execute({sqlText: logSql});
    }

        let frequencyArray = JSON.parse(fetch_frequency);
        let parsedFrequencyArray = `(${frequencyArray.map(f => `'${f}'`).join(", ")})`;

            var create_temp_table_command = `
                CREATE OR REPLACE TEMPORARY TABLE response_data AS 
                WITH api_call AS (
                    SELECT 
                        ethereum.streamline.udf_api('GET', CONCAT('https://api.github.com', full_endpoint), { 'Authorization': CONCAT('token ', '${token}'), 'Accept': 'application/vnd.github.v3+json' },{}) AS res,
                        CURRENT_TIMESTAMP AS _request_timestamp,
                        repo_url,
                        full_endpoint
                    FROM {{ target.database }}.silver.github_repos
                    WHERE (DATE(last_time_queried) <> CURRENT_DATE OR last_time_queried IS NULL)
                    AND frequency IN ${parsedFrequencyArray}
                    LIMIT 5000
                ),
                flatten_res AS (
                    SELECT 
                        repo_url,
                        full_endpoint,
                        res AS data,
                        'github' AS provider,
                        GET(data:headers, 'X-RateLimit-Remaining')::INTEGER as rate_limit_remaining,
                        _request_timestamp AS _inserted_timestamp,
                        CONCAT_WS('-', DATE_PART(epoch_second, _request_timestamp), SPLIT_PART(repo_url, '/', 2), full_endpoint) AS _res_id
                    FROM api_call
                )
                SELECT
                    repo_url,
                    full_endpoint,
                    data,
                    provider,
                    rate_limit_remaining,
                    _inserted_timestamp,
                    _res_id
                FROM
                flatten_res;
            `;

            snowflake.execute({sqlText: create_temp_table_command.replace('{fetch_frequency}', fetch_frequency, '{token}', token)});

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
                            SPLIT_PART(repo_url, '/', 1) as repo_owner,
                            SPLIT_PART(repo_url, '/', 2) as repo_name,
                            full_endpoint as endpoint_name,
                            data,
                            provider,
                            '${parsedFrequencyArray}' as frequency_type,
                            _inserted_timestamp,
                            _res_id
                        FROM response_data
                        WHERE rate_limit_remaining > 0;
            `;
            snowflake.execute({sqlText: insert_command.replace('{fetch_frequency}', parsedFrequencyArray)});

            // Update command: Update last_time_queried for the queried endpoints
            var update_command = `
                UPDATE {{ target.database }}.silver.github_repos
                SET last_time_queried = CURRENT_TIMESTAMP
                WHERE full_endpoint IN (SELECT full_endpoint FROM response_data WHERE rate_limit_remaining > 0);
            `;
            snowflake.execute({sqlText: update_command});

    return 'Data collection and update completed successfully.';

$$;
{% endset %}
{% do run_query(query) %}
{% endmacro %}