SHELL := /bin/bash

# set default target
DBT_TARGET ?= dev
AWS_LAMBDA_ROLE ?= aws_lambda_crosschain_api_dev

dbt-console: 
	docker-compose run dbt_console

.PHONY: dbt-console

prices_history:
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m 1+models/streamline/backfill/streamline__get_prices_history_ethereum.sql \
	--profile crosschain \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt

