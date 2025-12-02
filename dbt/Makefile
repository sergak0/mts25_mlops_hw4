DBT_PROJECT := .
DBT_PROFILE := transactions_clickhouse

.PHONY: deps run test build docs lint lint-fix full

deps:
	dbt deps

run:
	dbt run

test:
	dbt test

build:
	dbt build

docs:
	dbt docs generate

lint:
	sqlfluff lint models

lint-fix:
	sqlfluff fix models

full: deps seed build test

seed:
	dbt seed
