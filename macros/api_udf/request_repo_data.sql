{% macro request_repo_data() %}

{% set create_table %}
CREATE SCHEMA IF NOT EXISTS {{ target.database }}.bronze_api;
CREATE TABLE IF NOT EXISTS {{ target.database }}.bronze_api.github_repo_data(
    repo_owner STRING,
    repo_name STRING,
    endpoint_name STRING,
    type STRING,
    data VARIANT,
    provider STRING,
    endpoint_github STRING,
    _inserted_timestamp TIMESTAMP_NTZ,
    _res_id STRING
);
{% endset %}

{% do run_query(create_table) %}

{% set query %}
CREATE OR REPLACE PROCEDURE {{ target.database }}.bronze_api.getc_github_repo_data(fetch_frequency ARRAY, token STRING) 
    RETURNS STRING 
    LANGUAGE JAVASCRIPT 
    EXECUTE AS CALLER 
    AS $$


    function sleep(milliseconds) {
        var start = new Date().getTime();
        for (var i = 0; i < 1e7; i++) {
            if ((new Date().getTime() - start) > milliseconds) {
                break;
            }
        }
    }

    function logMessage(message) {
       let logSql = `SELECT SYSTEM$LOG('${message}')`;
       snowflake.execute({sqlText: logSql});
    }
        let parsedFrequencyArray = `('${FETCH_FREQUENCY.join("', '")}')`;
        var row_count = 0;
        var res = snowflake.execute({sqlText: `WITH subset as (
                SELECT *
                FROM CROSSCHAIN_DEV.silver.github_repos
                WHERE (DATE(last_time_queried) <> CURRENT_DATE OR last_time_queried IS NULL)
                AND frequency IN ${parsedFrequencyArray}
                LIMIT 10
            )
            SELECT count(*)
            FROM subset`});
        res.next()
        row_count = res.getColumnValue(1);
        
        call_groups = Math.ceil(row_count/100)
        
        for(let i = 0; i < call_groups; i++) {
            var create_temp_table_command = `
                CREATE OR REPLACE TEMPORARY TABLE CROSSCHAIN_DEV.bronze_api.response_data AS 
                WITH api_call AS (
                    SELECT 
                        livequery_dev.live.udf_api('GET', CONCAT('https://api.github.com', full_endpoint), { 'Authorization': CONCAT('token ', '${TOKEN}'), 'Accept': 'application/vnd.github+json' },{}) AS res,
                        CURRENT_TIMESTAMP AS _request_timestamp,
                        repo_url,
                        full_endpoint,
                        endpoint_github
                    FROM CROSSCHAIN_DEV.silver.github_repos
                    WHERE (DATE(last_time_queried) <> CURRENT_DATE OR last_time_queried IS NULL)
                    AND frequency IN ${parsedFrequencyArray}
                    LIMIT 100
                ),
                flatten_res AS (
                    SELECT 
                        repo_url,
                        full_endpoint,
                        res AS data,
                        'github' AS provider,
                        endpoint_github,
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
                INSERT INTO CROSSCHAIN_DEV.bronze_api.github_repo_data(
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
                            SPLIT_PART(repo_url, '/', 1) as repo_owner,
                            SPLIT_PART(repo_url, '/', 2) as repo_name,
                            full_endpoint as endpoint_name,
                            data,
                            provider,
                            endpoint_github,
                            _inserted_timestamp,
                            _res_id
                        FROM CROSSCHAIN_DEV.bronze_api.response_data
                        WHERE rate_limit_remaining > 0;
            `;
            snowflake.execute({sqlText: insert_command});
            // Update command: Update last_time_queried for the queried endpoints
            var update_command = `
                UPDATE CROSSCHAIN_DEV.silver.github_repos
                SET last_time_queried = CURRENT_TIMESTAMP
                WHERE full_endpoint IN (SELECT full_endpoint FROM CROSSCHAIN_DEV.bronze_api.response_data WHERE rate_limit_remaining > 0);
            `;
            snowflake.execute({sqlText: update_command});
        }
    return 'Success';

$$;
{% endset %}
{% do run_query(query) %}
{% endmacro %}