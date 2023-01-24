{% macro create_user_abi_table() -%}
    {% set create_query %}
create schema if not exists bronze_test;    
CREATE TABLE if NOT EXISTS bronze_test.user_abis(
        contract_address VARCHAR,
        blockchain VARCHAR,
        abi VARCHAR,
        discord_username VARCHAR,
        _INSERTED_TIMESTAMP TIMESTAMP,
        DUPLICATE_ABI BOOLEAN
    );
grant select on all tables in schema bronze_test to role datascience;
grant insert on all tables in schema bronze_test to role datascience;

{% endset %}
    {% do run_query(create_query) %}
{% endmacro -%}