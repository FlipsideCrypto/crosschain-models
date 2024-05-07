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
        var batch_size = 100;
        var max_batch_tries = 5;
        var num_calls_remaining = 5000;

        var res = snowflake.execute({sqlText: `WITH subset as (
                SELECT 
                    *,
                    row_number() over (order by full_endpoint) as rn, 
                    floor(rn/${batch_size}) as gn
                FROM {{ target.database }}.silver.github_repos
                WHERE (last_time_queried IS NULL OR DATE(last_time_queried) < DATEADD(DAY, -7, SYSDATE()))
                AND frequency IN ${parsedFrequencyArray}
            )
            SELECT *
            FROM subset
            WHERE gn < 10`}); /* limit 1000 endpoints per run due to GH rate limits */

        var query_id = res.getQueryId();

        var get_max_batch_num_stmt = snowflake.execute({sqlText: `select coalesce(max(gn),-1) from table(result_scan('${query_id}'))`});
        get_max_batch_num_stmt.next()
        var max_batch_num = get_max_batch_num_stmt.getColumnValue(1);

        for(let i = 0; i < max_batch_num+1; i++) {
            var is_batch_done = false
            var num_batch_tries = 0

            /* need at least (batch_size * max_batch_tries) number of calls left in our hourly rate limit for this batch to work*/
            if (num_calls_remaining < batch_size*max_batch_tries) {
                snowflake.execute({sqlText: `INSERT INTO {{ target.database }}.bronze_api.log_messages (log_level, message) VALUES ('INFO', 'CALLS REMAINING,ending execution due to ${num_calls_remaining} calls remaining')`});
                break;
            }

            /* seed the table for 1st run */
            snowflake.execute({sqlText: `create or replace temporary table {{ target.database }}.bronze_api.response_data AS
                select full_endpoint, NULL as status_code from table(result_scan('${query_id}'))`});

            while(!is_batch_done) {
                num_batch_tries = num_batch_tries + 1
                /* log start */
                snowflake.execute({sqlText: `INSERT INTO {{ target.database }}.bronze_api.log_messages (log_level, message) VALUES ('INFO', 'START,iteration ${num_batch_tries} of group ${i}')`});
                var create_temp_table_command = `
                    CREATE OR REPLACE TEMPORARY TABLE {{ target.database }}.bronze_api.response_data AS
                    WITH subset AS (
                        SELECT 
                            project_name,
                            SYSDATE() AS _request_timestamp,
                            repo_url,
                            full_endpoint,
                            endpoint_github,
                            row_number() over (order by full_endpoint) as subset_rn
                        FROM 
                            table(result_scan('${query_id}'))
                        WHERE 
                            gn = ${i}
                        AND 
                            full_endpoint in (SELECT full_endpoint FROM {{ target.database }}.bronze_api.response_data WHERE (status_code = 202 or status_code is NULL))
                    ),
                    api_call as (
                        SELECT
                            project_name,
                            {{ target.database }}.live.udf_api('GET', CONCAT('https://api.github.com', full_endpoint), { 'Authorization': CONCAT('token ', '{${TOKEN}}'), 'Accept': 'application/vnd.github+json'},{}, 'vault/github/api') AS res,
                            SYSDATE() AS _request_timestamp,
                            repo_url,
                            full_endpoint,
                            endpoint_github
                        FROM 
                            subset
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
                    flatten_res;`;

                snowflake.execute({sqlText: create_temp_table_command});
                
                var log_debug_status_code_message = `INSERT INTO {{ target.database }}.bronze_api.log_messages (log_level, message) select 'DEBUG', CONCAT_WS(',',full_endpoint,status_code) from {{ target.database }}.bronze_api.response_data`;
                snowflake.execute({sqlText: log_debug_status_code_message});

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

                var update_command = `
                    UPDATE {{ target.database }}.silver.github_repos
                    SET last_time_queried = SYSDATE(),
                    retries = 0
                    WHERE full_endpoint IN (SELECT full_endpoint FROM {{ target.database }}.bronze_api.response_data WHERE rate_limit_remaining > 0 and status_code != 202);
                `;
                snowflake.execute({sqlText: update_command});

                var num_calls_remaining_res = snowflake.execute({sqlText: `SELECT min(rate_limit_remaining) FROM {{ target.database }}.bronze_api.response_data;`});
                num_calls_remaining_res.next()
                num_calls_remaining = num_calls_remaining_res.getColumnValue(1);

                var need_to_retry_command = `SELECT full_endpoint FROM {{ target.database }}.bronze_api.response_data WHERE (status_code = 202 or status_code is NULL);`;
                var need_to_retry_res = snowflake.execute({sqlText: need_to_retry_command});
                var need_to_retry_row_count = need_to_retry_res.getRowCount();
                var wait_seconds = 5
                var wait_stmt = `CALL system$wait(${wait_seconds});`

                if (need_to_retry_row_count == 0 || num_batch_tries >= 5) {
                    is_batch_done = true
                } else if (need_to_retry_row_count > 10) {
                    snowflake.execute({sqlText: wait_stmt})
                }

                snowflake.execute({sqlText: `INSERT INTO {{ target.database }}.bronze_api.log_messages (log_level, message) VALUES ('INFO', 'CALLS REMAINING,${num_calls_remaining}')`});
                snowflake.execute({sqlText: `INSERT INTO {{ target.database }}.bronze_api.log_messages (log_level, message) VALUES ('INFO', 'END,iteration ${num_batch_tries} of group ${i}')`});
            }
        }

    return 'Success';

$$;
{% endset %}
{% do run_query(query) %}
{% endmacro %}