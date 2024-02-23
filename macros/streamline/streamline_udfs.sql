{% macro create_udf_bulk_rest_api() %}    
    {{ log("Creating udf get_chainhead for target:" ~ target.name ~ ", schema: " ~ target.schema, info=True) }}
    {{ log("role:" ~ target.role ~ ", user:" ~ target.user, info=True) }}
    {% set sql %}
    CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api_v2(json variant) returns variant api_integration = 
    {% if target.name == "prod" %} 
        {{ log("Creating prod udf_bulk_resp_api", info=True) }}
        aws_crosschain_api_prod AS 'https://35hm1qhag9.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_rest_api'
    {% elif target.name == "dev" %}
        {{ log("Creating dev udf_bulk_resp_api", info=True) }}
        aws_crosschain_api_stg AS 'https://q0bnjqvs9a.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api'
    {% elif  target.name == "sbx" %}
        {{ log("Creating sbx udf_bulk_resp_api", info=True) }}
    {%- endif %}
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}

{% macro generate_sl_udf_params(
    external_table='',
    sql_limit='5000',
    producer_batch_size='1000',
    worker_batch_size='1000',
    sm_secret_name=''
) %}
  {#
    This macro generates the parameters for the udf_bulk_rest_api_v2 function.

    Parameters:
    external_table (str): The name of the external table to flush. Defaults to ''.
    sql_limit (str, optional): The SQL limit. Defaults to '5000'.
    producer_batch_size (str, optional): The producer batch size. Defaults to '1000'.
    worker_batch_size (str, optional): The worker batch size. Defaults to '1000'.
    sm_secret_name (str): The secret name. Defaults to ''.
  #}
  {% raw %}{{this.schema}}{% endraw %}.udf_bulk_rest_api_v2(
    object_construct(
      'sql_source', '{% raw %}{{this.identifier}}{% endraw %}',
      'external_table', '{{external_table}}',
      'sql_limit', '{{sql_limit}}',
      'producer_batch_size', '{{producer_batch_size}}',
      'worker_batch_size', '{{worker_batch_size}}',
      'sm_secret_name', '{{sm_secret_name}}'
    )
  )
{% endmacro %}