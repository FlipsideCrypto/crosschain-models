{% macro sp_create_bulk_get_coin_market_cap_hourly_ohlc() %}
{% set sql %}
CREATE OR REPLACE PROCEDURE streamline.sp_bulk_get_coin_market_cap_hourly_ohlc() 
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
        streamline.all_unknown_coin_market_cap_asset_ohlc_hourly
    );
    if (
        row_cnt > 0
      ) THEN RESULT:= (
        SELECT
          streamline.udf_bulk_get_coin_market_cap_hourly_ohlc()
      );
      ELSE RESULT:= NULL;
    END if;
    RETURN RESULT;
  END;
$${% endset %}
{% do run_query(sql) %}
{% endmacro %}