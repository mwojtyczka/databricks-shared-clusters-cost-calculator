# Databricks notebook source
# MAGIC %md
# MAGIC # Run this notebook to setup additional sample data for the dashboard!

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
# MAGIC ## Populate User Info table

# COMMAND ----------

import random
from pyspark.sql.types import StructType, StructField, StringType

# Predefined department and cost center pairs
departments = [
    ("R&D"),
    ("FE"),
    ("PS")
]

users = spark.table(f"{catalog_and_schema}.cost_agg_day").select("user_name")

user_info_schema = StructType(
    [
        StructField("user_name", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("display_name", StringType(), False),
        StructField("organizational_entity_name", StringType(), False),
        StructField("organizational_entity_value", StringType(), True),
    ]
)

def create_user_info(data_df):
    user_info_list = []
    i = 0
    for row in data_df.collect():
        user_name = row["user_name"]
        user_id = i
        i = i+1
        display_name = user_name
        department = random.choice(departments)
        user_info_list.append((user_name, user_id, display_name, "department", department))

    return spark.createDataFrame(user_info_list, schema=user_info_schema)

user_info_df = create_user_info(users)
user_info_df.write.mode("overwrite").saveAsTable(f"{catalog_and_schema}.user_info")

# COMMAND ----------

display(user_info_df)

# COMMAND ----------




