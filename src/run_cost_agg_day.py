# Databricks notebook source

# COMMAND ----------

from clusters_cost_allocation.main import run_cost_agg_day

catalog = dbutils.widgets.get("output_catalog")
schema = dbutils.widgets.get("output_schema")

run_cost_agg_day(catalog, schema)
