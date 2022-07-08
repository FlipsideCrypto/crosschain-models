{% macro sp_create_bulk_get_coin_market_cap_prices() %}
{% set sql %}
CREATE OR REPLACE PROCEDURE silver.sp_bulk_get_coin_market_cap_prices() 
RETURNS variant 
LANGUAGE SQL 
AS 
$$
  DECLARE
    RESULT VARCHAR;
  BEGIN
    RESULT:= (
        SELECT
          silver.udf_bulk_get_coin_market_cap_prices()
      );
    RETURN RESULT;
  END;
$${% endset %}
{% do run_query(sql) %}
{% endmacro %}