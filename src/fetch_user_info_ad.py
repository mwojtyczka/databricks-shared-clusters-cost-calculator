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

# MAGIC %pip install databricks-sdk -U

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Setup authentication credentials for the Databricks Account
account_id = "xxx"
azure_client_id = "xxx"
azure_tenant_id = "xxx"

secrets_scope = "secrets"
secret_name = "dbx-secret"

secret_value = dbutils.secrets.get(secrets_scope, secret_name)

# Used for the MS GraphAPI to retrieve cost center
entra_id_token = ""

# COMMAND ----------

from databricks.sdk import AccountClient

acc = AccountClient(
    host="https://accounts.azuredatabricks.net/",
    account_id=account_id,
    azure_client_id=azure_client_id,
    azure_tenant_id=azure_tenant_id,
    azure_client_secret=secret_value,
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Get user info

# COMMAND ----------

import re
import requests

headers = {"Authorization": f"Bearer {entra_id_token}"}


def extract_business_unit_from_name(name: str) -> str:
    # Regular expression pattern to extract M
    pattern = r"\(([A-Za-z\,\s]*)\/"

    # Extract M using regex
    result = re.search(pattern, name)

    # Check if result is found and get the M
    if result:
        return result.group(1)

    print(f"Error for user {name}")
    return "MISSING_BU"


def get_cost_center_for_user(email: str) -> str:
    url = f"https://graph.microsoft.com/v1.0/users/{email}?$select=id,userPrincipalName,onPremisesExtensionAttributes"
    response = requests.get(url, headers=headers, timeout=360)
    return response.json()["onPremisesExtensionAttributes"]["extensionAttribute2"]


# COMMAND ----------

account_users = [
    {
        "user_name": u.user_name,
        "user_id": u.id,
        "display_name": u.display_name,
        "department": extract_business_unit_from_name(u.display_name),
        "cost_center": get_cost_center_for_user(u.user_name),
    }
    for u in acc.users.list()
]

# COMMAND ----------

from pyspark.sql.functions import col

user_info_df = spark.createDataFrame(account_users).where(
    col("cost_center").isNotNull()
)

user_info_df.where(col("cost_center").isNotNull()).write.format("delta").mode(
    "overwrite"
).saveAsTable(f"{catalog_and_schema}.user_info")
