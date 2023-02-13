{% macro sp_refresh_external_table_by_recent_date() %}
{% set sql %}
create or replace procedure streamline.refresh_external_table_by_recent_date(external_table_name string)
returns string
language sql
execute as caller
as
$$
    declare 
        path string;
        select_stmt string;
        refresh_stmt string;
        select_stmt2 string;
        refresh_stmt2 string;
        res resultset;
    begin 
        select_stmt := 'select concat(\'_inserted_date=\',current_date::string,\'/\') as path;';
        select_stmt2 := 'select concat(\'_inserted_date=\',(current_date-1)::string,\'/\') as path;';
        res := (execute immediate :select_stmt);
        let c1 cursor for res;
        for row_variable in c1 do
            path := row_variable.path;
        end for;
        refresh_stmt := 'alter external table streamline.{{ target.database }}.' || :external_table_name || ' refresh \'' || :PATH || '\'';
        res := (execute immediate :refresh_stmt);

        res := (execute immediate :select_stmt2);
        let c2 cursor for res;
        for row_variable in c2 do
            path := row_variable.path;
        end for;
        refresh_stmt2 := 'alter external table streamline.{{ target.database }}.' || :external_table_name || ' refresh \'' || :PATH || '\'';
        res := (execute immediate :refresh_stmt2);
        return 'table refreshed with ' || :refresh_stmt || ' and ' || :refresh_stmt2;
    end;
$${% endset %}
{% do run_query(sql) %}
{% endmacro %}