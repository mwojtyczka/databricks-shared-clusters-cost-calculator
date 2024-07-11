# Databricks notebook source

# COMMAND ----------

from cost_monitoring.main import calculate_monthly_costs

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

calculate_monthly_costs(catalog, schema)
