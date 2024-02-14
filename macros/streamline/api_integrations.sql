-- macro used to create crosschain sl 2.0 api integrations
{% macro create_aws_crosschain_api() %}
    {% if target.name == "prod" %}
        {{ log("Generating api integration for target:" ~ target.name, info=True) }}
        {% set sql %}
        
        CREATE api integration IF NOT EXISTS aws_crosschain_api_prod api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::924682671219:role/crosschain-api-prod-rolesnowflakeudfsAF733095-hN5B5eoGh80E' api_allowed_prefixes = (
            'https://y4vgsb5jk5.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;    
        {% endset %}
        {% do adapter.execute(sql) %}
    {% elif target.name == "dev" %}
        {{ log("Generating api integration for target:" ~ target.name, info=True) }}
        {% set sql %}
        
        CREATE api integration IF NOT EXISTS aws_crosschain_api_stg api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/crosschain-api-stg-rolesnowflakeudfsAF733095-5u61kmZdVfGr' api_allowed_prefixes = (
            'https://q0bnjqvs9a.execute-api.us-east-1.amazonaws.com/dev/'
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




