{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS", False) %}
        {% set sql %}
            CREATE schema if NOT EXISTS streamline;
            {{ create_udf_hex_to_int(
            schema = "public"
        ) }}
        {{ create_udf_hex_to_int_with_inputs(
            schema = "public"
        ) }}
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
