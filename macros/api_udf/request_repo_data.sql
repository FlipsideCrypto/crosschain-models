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

{% set event_table %}

CREATE TABLE IF NOT EXISTS {{ target.database }}.bronze_api.log_messages (
    timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    log_level STRING,
    message STRING
);
{% endset %}

{% do run_query(event_table) %}
{% do run_query(create_table) %}

{% set query %}
CREATE OR REPLACE PROCEDURE {{ target.database }}.bronze_api.get_github_api_repo_data(fetch_frequency ARRAY, token STRING) 
    RETURNS STRING 
    LANGUAGE JAVASCRIPT 
    EXECUTE AS CALLER 
    AS $$

        let parsedFrequencyArray = `('${FETCH_FREQUENCY.join("', '")}')`;
        const MIN_BATCH_SIZE = 10;  
        const MAX_BATCH_SIZE = 100;

        var row_count = 0;
        var batch_num = 100;
        var max_calls = 5000;

        var res = snowflake.execute({sqlText: `WITH subset as (
                SELECT *
                FROM {{ target.database }}.silver.github_repos
                WHERE  (last_time_queried IS NULL OR DATE(last_time_queried) < DATEADD(DAY, -7, SYSDATE()))
                AND frequency IN ${parsedFrequencyArray}
                LIMIT 5000
            )
            SELECT count(*)
            FROM subset`});
        res.next()
        row_count = res.getColumnValue(1);
        
        if (row_count < 1000) {
            batch_num = 100;
        } else {
            batch_num = Math.max(MIN_BATCH_SIZE, Math.min(MAX_BATCH_SIZE, Math.round(row_count * 0.05)));
        }

        call_groups = Math.round(max_calls/batch_num);

        for(let i = 0; i < call_groups; i++) {

            var create_temp_table_command = `
                CREATE OR REPLACE TEMPORARY TABLE {{ target.database }}.bronze_api.response_data AS
                WITH api_call AS (
            `;

            for (let i = 0; i < batch_num; i++) {
                create_temp_table_command += `
                    SELECT * FROM (
                        SELECT
                            project_name,
                            {{ target.database }}.live.udf_api('GET', CONCAT('https://api.github.com', full_endpoint), { 'Authorization': CONCAT('token ', '{${TOKEN}}'), 'Accept': 'application/vnd.github+json'},{}, 'github_cred') AS res,
                            SYSDATE() AS _request_timestamp,
                            repo_url,
                            full_endpoint,
                            endpoint_github
                        FROM {{ target.database }}.silver.github_repos
                        WHERE     (last_time_queried IS NULL OR DATE(last_time_queried) < DATEADD(DAY, -7, SYSDATE()))
                        AND frequency IN ${parsedFrequencyArray}
                        ORDER BY full_endpoint, retries DESC
                        LIMIT 1 OFFSET ${i}
                    )
                `;

                if (i < batch_num - 1) {
                    create_temp_table_command += `  UNION ALL `;
                }
            }

            create_temp_table_command += `
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
                        WHERE rate_limit_remaining > 0 and status_code != 202;
            `;
            snowflake.execute({sqlText: insert_command});
            // Update command: Update last_time_queried for the queried endpoints
            var update_command = `
                UPDATE {{ target.database }}.silver.github_repos
                SET last_time_queried = SYSDATE(),
                retries = 0
                WHERE full_endpoint IN (SELECT full_endpoint FROM {{ target.database }}.bronze_api.response_data WHERE rate_limit_remaining > 0 and status_code != 202);
            `;
            snowflake.execute({sqlText: update_command});

            var check_retries_command = `
                SELECT COUNT(*) 
                FROM {{ target.database }}.silver.github_repos 
                WHERE retries > 5
            `;
            var result_set = snowflake.execute({sqlText: check_retries_command});
            result_set.next();
            var count_rows_with_excessive_retries = result_set.getColumnValue(1);



            var update_retries_command = `
                UPDATE {{ target.database }}.silver.github_repos
                SET retries = retries + 1
                WHERE full_endpoint IN (SELECT full_endpoint FROM {{ target.database }}.bronze_api.response_data WHERE status_code = 202);
            `;
            snowflake.execute({sqlText: update_retries_command});
            
            if (count_rows_with_excessive_retries > 0) {
                var reset_retries_command = `
                    UPDATE {{ target.database }}.silver.github_repos
                    SET retries = 0
                    WHERE retries > 5
                `;
                snowflake.execute({sqlText: reset_retries_command});
            }

            // Check if there are any rows with non-zero retries
            var check_retries_query = `
                SELECT COUNT(*) as count_rows
                FROM {{ target.database }}.silver.github_repos
                WHERE retries > 0
            `;
            var result_set_check = snowflake.execute({sqlText: check_retries_query});
            result_set_check.next();
            var count_rows_with_retries = result_set_check.getColumnValue(1);

            if (count_rows_with_retries > 0) {
                    // Fetch the retries value of the first row ordered by retries DESC
                    var retries_query = `
                        SELECT AVG(*) FROM ( SELECT retries
                        FROM {{ target.database }}.silver.github_repos
                        ORDER BY retries DESC
                        LIMIT ${batch_num})
                    `;
                    var result_set = snowflake.execute({sqlText: retries_query});
                    result_set.next();
                    var retries = result_set.getColumnValue(1);

                    // Determine the wait time based on the retries value
                    // For example, if you want to wait 10 seconds for each retry:
                    var wait_time = Math.round(retries);

                    var wait_command = `
                        CALL system$wait(${wait_time});
                    `;


                snowflake.execute({sqlText: wait_command});
            }

            else if (count_rows_with_retries = 0) {
                    break;
                }

            var log_message = `INSERT INTO {{ target.database }}.bronze_api.log_messages (log_level, message) VALUES ('INFO', ' Iteration ${i} of ${call_groups} complete.')`;
            snowflake.execute({sqlText: log_message});
            }

    return 'Success';

$$;
{% endset %}
{% do run_query(query) %}
{% endmacro %}