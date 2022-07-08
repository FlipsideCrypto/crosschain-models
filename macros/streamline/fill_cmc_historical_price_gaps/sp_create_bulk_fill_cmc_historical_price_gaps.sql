{% macro sp_create_bulk_fill_cmc_historical_price_gaps() %}
{% set sql %}
CREATE OR REPLACE PROCEDURE silver.sp_bulk_fill_cmc_historical_price_gaps() 
RETURNS variant 
LANGUAGE SQL 
AS 
$$
  DECLARE
    RESULT VARCHAR;
  BEGIN
    RESULT:= (
        SELECT
          silver.udf_bulk_fill_cmc_historical_price_gaps()
      );
    RETURN RESULT;
  END;
$${% endset %}
{% do run_query(sql) %}
{% endmacro %}