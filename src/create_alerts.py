# Databricks notebook source
# pylint: skip-file
# COMMAND ----------
# MAGIC %md
# MAGIC ## Install databricks-sdk
# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get catalog and schema

# COMMAND ----------

dbutils.widgets.text("output_catalog", "main")
dbutils.widgets.text("output_schema", "billing_usage_granular")
dbutils.widgets.text("alert_prefix", "granular-billing-budget")

catalog = dbutils.widgets.get("output_catalog")
schema = dbutils.widgets.get("output_schema")
alert_prefix = dbutils.widgets.get("alert_prefix")

catalog_and_schema = f"{catalog}.{schema}"
print(f"Use {catalog_and_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define functions

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from clusters_cost_allocation.dbsql_handler import SqlObjectsHandler
from clusters_cost_allocation.dbsql_queries import (
    get_dbu_cost_alert_query_body,
    get_cloud_cost_alert_query_body,
)

w = WorkspaceClient()
handler = SqlObjectsHandler(w)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove existing Queries and Alerts

# COMMAND ----------

alert_name_dbu_cost = alert_prefix + "-dbu-cost"
query_name_dbu_cost = alert_prefix + "-dbu-cost"

alert_name_cloud_cost = alert_prefix + "-cloud-cost"
query_name_cloud_cost = alert_prefix + "-cloud-cost"

handler.delete_query_and_alert(query_name_dbu_cost, alert_name_dbu_cost)
handler.delete_query_and_alert(query_name_cloud_cost, alert_name_cloud_cost)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Queries and Alerts
# MAGIC
# MAGIC Alert is raised if costs are exceeded for any of the departments.
# MAGIC There is a separate alert for DBU and Cloud costs.

# COMMAND ----------

handler.create_query_and_alert(
    query_name_dbu_cost,
    get_dbu_cost_alert_query_body(catalog_and_schema),
    "DBU Budget Query",
    alert_name_dbu_cost,
)
handler.create_query_and_alert(
    query_name_cloud_cost,
    get_cloud_cost_alert_query_body(catalog_and_schema),
    "Cloud Budget Query",
    alert_name_cloud_cost,
)

# COMMAND ----------
