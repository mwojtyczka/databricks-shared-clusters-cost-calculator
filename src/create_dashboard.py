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
dbutils.widgets.text("dashboard_name", "Granular Cost Allocation for Shared Clusters")
dbutils.widgets.text("dashboard_file_path", "../lake_view/dashboard_template.json")

catalog = dbutils.widgets.get("output_catalog")
schema = dbutils.widgets.get("output_schema")
dashboard_name = dbutils.widgets.get("dashboard_name")
dashboard_file_path = dbutils.widgets.get("dashboard_file_path")

catalog_and_schema = f"{catalog}.{schema}"
print(f"Use {catalog_and_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Re)-create Lake View Dashboard

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from clusters_cost_allocation.dbsql_handler import SqlObjectsHandler


def read_file(file_path: str, catalog_and_schema: str):
    with open(file_path, "r") as file:
        file_content = file.read()

    return file_content.replace("{catalog_and_schema}", catalog_and_schema)


w = WorkspaceClient()
handler = SqlObjectsHandler(w)

handler.delete_dashboard(dashboard_name)
dashboard_body = read_file(dashboard_file_path, catalog_and_schema)
dashboard = handler.create_dashboard(
    name=dashboard_name, serialized_dashboard=dashboard_body
)
# handler.publish(dashboard.dashboard_id)

# COMMAND ----------
