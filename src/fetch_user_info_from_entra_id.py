# Databricks notebook source
# pylint: skip-file
# COMMAND ----------
# MAGIC %md
# MAGIC # Use this notebook to fetch user info from Microsoft Entra ID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define output catalog and schema

# COMMAND ----------

dbutils.widgets.text("output_catalog", "main")
dbutils.widgets.text("output_schema", "billing_usage_granular")

catalog = dbutils.widgets.get("output_catalog")
schema = dbutils.widgets.get("output_schema")

catalog_and_schema = f"{catalog}.{schema}"
print(f"Use {catalog_and_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate token for Graph API:
# MAGIC `az account get-access-token --resource-type ms-graph`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Databricks client

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.32.1 -U

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Retrieve authentication credentials
account_id = "xxx"
azure_client_id = "xxx"
azure_tenant_id = "xxx"

secrets_scope = "secrets"
azure_client_secret = dbutils.secrets.get(secrets_scope, "azure_client_secret")
entra_id_auth_token = dbutils.secrets.get(secrets_scope, "entra_id_auth_token")

# COMMAND ----------

from databricks.sdk import AccountClient

acc = AccountClient(
    host="https://accounts.azuredatabricks.net/",
    account_id=account_id,
    azure_client_id=azure_client_id,
    azure_tenant_id=azure_tenant_id,
    azure_client_secret=azure_client_secret,
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Get user info

# COMMAND ----------

import requests

headers = {"Authorization": f"Bearer {entra_id_auth_token}"}


def get_department_for_user(email: str) -> str:
    url = f"https://graph.microsoft.com/v1.0/users/{email}?$select=id,userPrincipalName,onPremisesExtensionAttributes"
    response = requests.get(url, headers=headers, timeout=360)
    return response.json()["onPremisesExtensionAttributes"]["extensionAttribute2"]


# COMMAND ----------

# consider only users that are added to Databricks account
account_users = [
    {
        "user_name": u.user_name,
        "user_id": u.id,
        "display_name": u.display_name,
        "organizational_entity_name": "department",
        "organizational_entity_value": get_department_for_user(u.display_name),
    }
    for u in acc.users.list()
]

# COMMAND ----------

from pyspark.sql.functions import col

user_info_df = spark.createDataFrame(account_users).where(
    col("organizational_entity_value").isNotNull()
)

user_info_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_and_schema}.user_info"
)

# COMMAND ----------

display(user_info_df)

# COMMAND ----------
