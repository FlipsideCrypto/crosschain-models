{% macro sp_create_bulk_fill_cmc_historical_price_gaps() %}
{% set sql %}
CREATE OR REPLACE PROCEDURE silver.sp_bulk_fill_cmc_historical_price_gaps() 
RETURNS variant 
LANGUAGE SQL 
AS 
$$
  DECLARE
    RESULT VARCHAR;
    row_cnt INTEGER;
  BEGIN
    row_cnt:= (
      SELECT
        COUNT(1)
      FROM
        silver.legacy_prices_gaps
    );
    if (
        row_cnt > 0
      ) THEN RESULT:= (
        SELECT
          silver.udf_bulk_fill_cmc_historical_price_gaps()
      );
      ELSE RESULT:= NULL;
    END if;
    RETURN RESULT;
  END;
$${% endset %}
{% do run_query(sql) %}
{% endmacro %}