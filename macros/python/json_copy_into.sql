{% macro load_json_tbl(file_name) %}

  CREATE TABLE IF NOT EXISTS crosschain.bronze.json_config_tbl (
      data VARIANT,
      _inserted_timestamp TIMESTAMP_LTZ DEFAULT SYSDATE()
  );

  COPY INTO crosschain.bronze.json_config_tbl(data)
  FROM @json_config_stage/{{ file_name }}
  FILE_FORMAT = (TYPE = 'JSON')
  ON_ERROR = 'CONTINUE';

{% endmacro %}