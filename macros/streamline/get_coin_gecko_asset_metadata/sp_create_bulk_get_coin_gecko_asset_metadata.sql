{% macro sp_create_bulk_get_coin_gecko_asset_metadata() %}
{% set sql %}
CREATE OR REPLACE PROCEDURE silver.sp_bulk_get_coin_gecko_asset_metadata() 
RETURNS variant 
LANGUAGE SQL 
AS 
$$
  DECLARE
    RESULT VARCHAR;
  BEGIN
    RESULT:= (
        SELECT
          silver.udf_bulk_get_coin_gecko_asset_metadata()
      );
    RETURN RESULT;
  END;
$${% endset %}
{% do run_query(sql) %}
{% endmacro %}