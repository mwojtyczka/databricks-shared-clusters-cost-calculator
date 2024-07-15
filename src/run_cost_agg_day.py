# Databricks notebook source

# COMMAND ----------

from clusters_cost_allocation.main import run_cost_agg_day

catalog = dbutils.widgets.get("output_catalog")  # pylint:disable=undefined-variable
schema = dbutils.widgets.get("output_schema")  # pylint:disable=undefined-variable

run_cost_agg_day(catalog, schema)
