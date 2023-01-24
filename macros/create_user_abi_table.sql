{% macro create_user_abi_table() -%}
    {% set create_query %}
create schema if not exists bronze_public;    
CREATE TABLE if NOT EXISTS bronze_public.user_abis(
        contract_address VARCHAR,
        blockchain VARCHAR,
        abi VARCHAR,
        discord_username VARCHAR,
        _INSERTED_TIMESTAMP TIMESTAMP,
        DUPLICATE_ABI BOOLEAN
    );
grant select on all tables in schema bronze_public to role datascience;
grant insert on all tables in schema bronze_public to role datascience;

{% endset %}
    {% do run_query(create_query) %}
{% endmacro -%}