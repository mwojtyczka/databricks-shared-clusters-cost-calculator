# Databricks notebook source

# COMMAND ----------

from cost_monitoring.main import calculate_daily_costs

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

calculate_daily_costs(catalog, schema)
