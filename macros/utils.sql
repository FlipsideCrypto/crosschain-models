{% macro if_data_call_function(
        func,
        target
    ) %}
    {% if var(
            "STREAMLINE_INVOKE_STREAMS"
        ) %}
        {% if execute %}
            {{ log(
                "Running macro `if_data_call_function`: Calling udf " ~ func ~ " on " ~ target,
                True
            ) }}
        {% endif %}
    SELECT
        {{ func }}
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                {{ target }}
            LIMIT
                1
        )
    {% else %}
        {% if execute %}
            {{ log(
                "Running macro `if_data_call_function`: NOOP",
                False
            ) }}
        {% endif %}
    SELECT
        NULL
    {% endif %}
{% endmacro %}

{% macro if_data_call_wait() %}
    {% if var(
            "STREAMLINE_INVOKE_STREAMS"
        ) %}
        {% set query %}
    SELECT
        1
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                {{ model.schema ~ "." ~ model.alias }}
            LIMIT
                1
        ) {% endset %}
        {% if execute %}
            {% set results = run_query(
                query
            ) %}
            {% if results %}
                {{ log(
                    "Waiting...",
                    info = True
                ) }}

                {% set wait_query %}
            SELECT
                system$wait(
                    {{ var(
                        "WAIT",
                        600
                    ) }}
                ) {% endset %}
                {% do run_query(wait_query) %}
            {% else %}
            SELECT
                NULL;
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}

-- macro used to run a sql query and log the results
-- TODO: Move this to fsc-utils package
{% macro run_and_log_sql(sql_query, log_level='info') %}
    {% set result_var = 'result_' ~ sql_query[:8] %}
    
    {% set log_message = 'Executing SQL query: ' ~ sql_query %}
    {% do log(log_message,info=True) %}
    
    {% set query_result = run_query(sql_query) %}
    {% set result_str = query_result.columns[0].values()[0] if query_result.columns else None %}
    
    {% set log_message = 'SQL query result: ' ~ result_str %}
    {% do log(log_message, info=True) %}
    
    {{ result_var }}
{% endmacro %}

-- macro used to grant streamline priveleges to a role
{% macro grant_streamline_privileges(role) %}
    {{ log("Granting privileges to role: " ~ role, info=True) }}
    {% set sql %}
        grant usage on database {{ target.database }} to role {{ role }};
        grant usage on schema {{ target.schema }} to role {{ role }};
        grant usage on warehouse {{ target.warehouse }} to role {{ role }};
        grant select on all tables in schema {{ target.schema }} to role {{ role }};
        grant select on all views in schema {{ target.schema }} to role {{ role }};
        grant select on future views in schema {{ target.schema }} to role {{ role }};

        grant usage on database streamline to role {{ role }};
        grant usage on schema streamline.{{ target.schema }} to role {{ role }};
        grant select on all tables in schema streamline.{{ target.schema }} to role {{ role }};

        grant usage on schema {{target.database}}.bronze to role {{ role }};
    {% endset %}

    {% do run_and_log_sql(sql) %}
    {% do log("Privileges granted", info=True) %}
{% endmacro %}
