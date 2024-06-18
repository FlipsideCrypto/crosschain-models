{% macro run_sp_create_prod_clone() %}
    {% set clone_query %}
    call crosschain._internal.create_prod_clone(
        'crosschain',
        'crosschain_dev',
        'internal_dev'
    );
{% endset %}
    {% do run_query(clone_query) %}
{% endmacro %}
