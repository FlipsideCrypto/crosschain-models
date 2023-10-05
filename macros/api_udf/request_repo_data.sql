{% macro request_repo_data() %}

{% set create_table %}
CREATE SCHEMA IF NOT EXISTS {{ target.database }}.bronze_api;
CREATE TABLE IF NOT EXISTS {{ target.database }}.bronze_api.github_repo_data(
    project_name STRING,
    repo_owner STRING,
    repo_name STRING,
    endpoint_name STRING,
    data VARIANT,
    provider STRING,
    endpoint_github STRING,
    _inserted_timestamp TIMESTAMP_NTZ,
    _res_id STRING
);
{% endset %}

{% do run_query(create_table) %}

{% set query %}
CREATE OR REPLACE PROCEDURE {{ target.database }}.bronze_api.get_github_api_repo_data(fetch_frequency ARRAY, token STRING) 
    RETURNS STRING 
    LANGUAGE JAVASCRIPT 
    EXECUTE AS CALLER 
    AS $$

        let parsedFrequencyArray = `('${FETCH_FREQUENCY.join("', '")}')`;
        
        for(let i = 0; i < 1000; i++) {
            var create_temp_table_command = `
                CREATE OR REPLACE TEMPORARY TABLE {{ target.database }}.bronze_api.response_data AS
                WITH api_call AS (
                    SELECT
                        project_name,
                        livequery_dev.live.udf_api('GET', CONCAT('https://api.github.com', full_endpoint), { 'Authorization': CONCAT('token ', '{${TOKEN}}'), 'Accept': 'application/vnd.github+json'},{}, 'github_cred') AS res,
                        SYSDATE() AS _request_timestamp,
                        repo_url,
                        full_endpoint,
                        endpoint_github
                    FROM {{ target.database }}.silver.github_repos
                    WHERE (DATE(last_time_queried) <> SYSDATE()::DATE OR last_time_queried IS NULL)
                    AND frequency IN ${parsedFrequencyArray}
                    ORDER BY retries DESC
                    LIMIT 1

                    UNION ALL

                    SELECT
                        project_name,
                        livequery_dev.live.udf_api('GET', CONCAT('https://api.github.com', full_endpoint), { 'Authorization': CONCAT('token ', '{${TOKEN}}'), 'Accept': 'application/vnd.github+json'},{}, 'github_cred') AS res,
                        SYSDATE() AS _request_timestamp,
                        repo_url,
                        full_endpoint,
                        endpoint_github
                    FROM {{ target.database }}.silver.github_repos
                    WHERE (DATE(last_time_queried) <> SYSDATE()::DATE OR last_time_queried IS NULL)
                    AND frequency IN ${parsedFrequencyArray}
                    ORDER BY retries DESC
                    LIMIT 1

                    UNION ALL

                    SELECT
                        project_name,
                        livequery_dev.live.udf_api('GET', CONCAT('https://api.github.com', full_endpoint), { 'Authorization': CONCAT('token ', '{${TOKEN}}'), 'Accept': 'application/vnd.github+json'},{}, 'github_cred') AS res,
                        SYSDATE() AS _request_timestamp,
                        repo_url,
                        full_endpoint,
                        endpoint_github
                    FROM {{ target.database }}.silver.github_repos
                    WHERE (DATE(last_time_queried) <> SYSDATE()::DATE OR last_time_queried IS NULL)
                    AND frequency IN ${parsedFrequencyArray}
                    ORDER BY retries DESC
                    LIMIT 1

                    UNION ALL

                    SELECT
                        project_name,
                        livequery_dev.live.udf_api('GET', CONCAT('https://api.github.com', full_endpoint), { 'Authorization': CONCAT('token ', '{${TOKEN}}'), 'Accept': 'application/vnd.github+json'},{}, 'github_cred') AS res,
                        SYSDATE() AS _request_timestamp,
                        repo_url,
                        full_endpoint,
                        endpoint_github
                    FROM {{ target.database }}.silver.github_repos
                    WHERE (DATE(last_time_queried) <> SYSDATE()::DATE OR last_time_queried IS NULL)
                    AND frequency IN ${parsedFrequencyArray}
                    ORDER BY retries DESC
                    LIMIT 1

                    UNION ALL

                    SELECT
                        project_name,
                        livequery_dev.live.udf_api('GET', CONCAT('https://api.github.com', full_endpoint), { 'Authorization': CONCAT('token ', '{${TOKEN}}'), 'Accept': 'application/vnd.github+json'},{}, 'github_cred') AS res,
                        SYSDATE() AS _request_timestamp,
                        repo_url,
                        full_endpoint,
                        endpoint_github
                    FROM {{ target.database }}.silver.github_repos
                    WHERE (DATE(last_time_queried) <> SYSDATE()::DATE OR last_time_queried IS NULL)
                    AND frequency IN ${parsedFrequencyArray}
                    ORDER BY retries DESC
                    LIMIT 1

                ),
                flatten_res AS (
                    SELECT
                        project_name, 
                        repo_url,
                        full_endpoint,
                        res:data AS data,
                        res:status_code AS status_code,
                        'github' AS provider,
                        endpoint_github,
                        GET(res:headers, 'X-RateLimit-Remaining')::INTEGER as rate_limit_remaining,
                        _request_timestamp AS _inserted_timestamp,
                        CONCAT_WS('-', DATE_PART(epoch_second, _request_timestamp), SPLIT_PART(repo_url, '/', 2), full_endpoint) AS _res_id
                    FROM api_call
                )
                SELECT
                    project_name,
                    repo_url,
                    full_endpoint,
                    data,
                    status_code,
                    provider,
                    endpoint_github,
                    rate_limit_remaining,
                    _inserted_timestamp,
                    _res_id
                FROM
                flatten_res;
            `;

            snowflake.execute({sqlText: create_temp_table_command});
            // Second command: Insert data into the target table from the temporary table
            
            var insert_command = `
                INSERT INTO {{ target.database }}.bronze_api.github_repo_data(
                            project_name,
                            repo_owner,
                            repo_name,
                            endpoint_name,
                            data,
                            provider,
                            endpoint_github,
                            _inserted_timestamp,
                            _res_id
                        )
                        SELECT
                            project_name,
                            SPLIT_PART(repo_url, '/', 1) as repo_owner,
                            SPLIT_PART(repo_url, '/', 2) as repo_name,
                            full_endpoint as endpoint_name,
                            data,
                            provider,
                            endpoint_github,
                            _inserted_timestamp,
                            _res_id
                        FROM {{ target.database }}.bronze_api.response_data
                        WHERE rate_limit_remaining > 0 and status_code = 200;
            `;
            snowflake.execute({sqlText: insert_command});
            // Update command: Update last_time_queried for the queried endpoints
            var update_command = `
                UPDATE {{ target.database }}.silver.github_repos
                SET last_time_queried = SYSDATE()
                WHERE full_endpoint IN (SELECT full_endpoint FROM {{ target.database }}.bronze_api.response_data WHERE rate_limit_remaining > 0 and status_code = 200);
            `;
            snowflake.execute({sqlText: update_command});

            var update_retries_command = `
                UPDATE {{ target.database }}.silver.github_repos
                SET retries = retries + 1
                WHERE full_endpoint IN (SELECT full_endpoint FROM {{ target.database }}.bronze_api.response_data WHERE status_code = 202);
            `;
            snowflake.execute({sqlText: update_retries_command});

            var avg_retries_query = `
                SELECT AVG(retries) as avg_retries
                FROM {{ target.database }}.silver.github_repos
                WHERE full_endpoint IN (SELECT full_endpoint FROM {{ target.database }}.bronze_api.response_data WHERE status_code = 202);
            `;
            var result_set = snowflake.execute({sqlText: avg_retries_query});
            result_set.next();
            var avg_retries = result_set.getColumnValue(1);

            // system wait based on average retries
            var wait_time = 20 * avg_retries;  // Adjust this formula as needed
            var wait_command = `
                CALL system$wait(${wait_time});
            `;

            snowflake.execute({sqlText: wait_command});
        }

        // reset retries to 0 when im done

        var reset_retries_command = `
            UPDATE {{ target.database }}.silver.github_repos
            SET retries = 0
            WHERE DATE(last_time_queried) = SYSDATE()::DATE AND frequency IN ${parsedFrequencyArray};
        `;


    return 'Success';

$$;
{% endset %}
{% do run_query(query) %}
{% endmacro %}