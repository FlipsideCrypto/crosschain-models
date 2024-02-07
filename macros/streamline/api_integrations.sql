-- macro used to create flow api integrations
{% macro create_aws_crosschain_api() %}
    {% if target.name == "prod" %}
        {# pass #}
    {% elif target.name == "dev" %}
        {{ log("Generating api integration for target:" ~ target.name, info=True) }}
        {% set sql %}
        
        CREATE api integration IF NOT EXISTS aws_crosschain_api_dev_stg api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/crosschain-api-dev-rolesnowflakeudfsAF733095-JKJPFBDhuynL' api_allowed_prefixes = (
            'https://skzzs32zdb.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;    
        {% endset %}
        {% do adapter.execute(sql) %}

        {{ log("Generating api integration for target:" ~ target.name, info=True) }}
        {% set sql %}
        
        CREATE api integration IF NOT EXISTS aws_crosschain_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/snowflake-api-crosschain' api_allowed_prefixes = (
            'https://tlbh2d47i2.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;    
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
