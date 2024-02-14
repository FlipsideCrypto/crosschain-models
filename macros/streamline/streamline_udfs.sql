{% macro create_udf_bulk_rest_api() %}    
    {{ log("Creating udf get_chainhead for target:" ~ target.name ~ ", schema: " ~ target.schema, info=True) }}
    {{ log("role:" ~ target.role ~ ", user:" ~ target.user, info=True) }}
    {% set sql %}
    CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api_v2(json variant) returns variant api_integration = 
    {% if target.name == "prod" %} 
        {{ log("Creating prod udf_bulk_resp_api", info=True) }}
    {% elif target.name == "dev" %}
        {{ log("Creating dev udf_bulk_resp_api", info=True) }}
        aws_crosschain_api_dev_stg AS 'https://skzzs32zdb.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_rest_api'
    {% elif  target.name == "sbx" %}
        {{ log("Creating sbx udf_bulk_resp_api", info=True) }}
    {%- endif %}
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
