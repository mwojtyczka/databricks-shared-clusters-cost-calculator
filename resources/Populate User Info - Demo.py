# Databricks notebook source
# Sample predefined lists for department and cost_center
import random
from pyspark.sql.types import StructType, StructField, StringType

# Predefined department and cost center pairs
department_cost_center_pairs = [
    ("R&D", "701"),
    ("Field Eng", "702"),
    ("PS", "703")
]

users = spark.table("main.billing_clusters_cost_allocation.cost_agg_day").select("user_name")

# Define user_info_schema
user_info_schema = StructType(
    [
        StructField("user_name", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("display_name", StringType(), False),
        StructField("department", StringType(), False),
        StructField("cost_center", StringType(), True),
    ]
)

# Generate user info DataFrame
def create_user_info(data_df):
    user_info_list = []
    i = 0
    for row in data_df.collect():
        user_name = row["user_name"]
        user_id = i
        i = i+1
        display_name = user_name
        department, cost_center = random.choice(department_cost_center_pairs)
        user_info_list.append((user_name, user_id, display_name, department, cost_center))

    return spark.createDataFrame(user_info_list, schema=user_info_schema)

user_info_df = create_user_info(users)

# Show the result
user_info_df.show()


user_info_df.write.mode("overwrite").saveAsTable("main.billing_clusters_cost_allocation.user_info")

# COMMAND ----------

display(spark.sql('select * from main.billing_clusters_cost_allocation.user_info'))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.billing_clusters_cost_allocation.budgets(
# MAGIC   department string NOT NULL,
# MAGIC   monthly_budget long NOT NULL,
# MAGIC   weekly_budget long NOT NULL,
# MAGIC   CONSTRAINT budgets_pk PRIMARY KEY(department)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO main.billing_clusters_cost_allocation.budgets(department, monthly_budget, weekly_budget)
# MAGIC SELECT "R&D", 400, 70
# MAGIC UNION
# MAGIC SELECT "FE", 300, 60
# MAGIC UNION
# MAGIC SELECT "PS", 200, 50

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.billing_clusters_cost_allocation.budgets
