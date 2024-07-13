# Databricks notebook source

# COMMAND ----------

from clusters_cost_allocation.main import run_cost_agg_day

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

run_cost_agg_day(catalog, schema)
