SHELL := /bin/bash

# set default target
DBT_TARGET ?= dev
AWS_LAMBDA_ROLE ?= aws_lambda_crosschain_api_dev
INVOKE_STREAMS ?= True

dbt-console: 
	docker-compose run dbt_console

.PHONY: dbt-console

sl-api-integrations:
	dbt run-operation create_aws_crosschain_api \
	--profile crosschain \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt/

udfs:
	dbt run-operation create_udf_bulk_rest_api_v2 \
	--vars '{"UPDATE_UDFS_AND_SPS":True}' \
	--profile crosschain \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt/

streamline-v2: sl-api-integrations udfs

prices_history:
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS": $(INVOKE_STREAMS), "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m "crosschain_models,tag:streamline_prices_complete" "crosschain_models,tag:streamline_prices_history" \
	--profile crosschain \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt

ohlc_realtime:
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS": $(INVOKE_STREAMS), "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m "crosschain_models,tag:ohlc_realtime_v2" \
	--profile crosschain \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt