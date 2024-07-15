-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Setting up schema

-- COMMAND ----------

USE CATALOG ${output_catalog};

-- COMMAND ----------

-- MAGIC %python
-- MAGIC schema = dbutils.widgets.get("output_schema")
-- MAGIC if dbutils.widgets.get("recreate_schema").lower() == "true":
-- MAGIC     print(f"Dropping schema: {schema}")
-- MAGIC     spark.sql(f"DROP SCHEMA IF EXISTS {schema} cascade")
-- MAGIC
-- MAGIC print(f"Creating schema: {schema}")

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${output_schema};

-- COMMAND ----------

USE SCHEMA ${output_schema};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating Tables

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_info(
  user_name string NOT NULL,
  user_id string NOT NULL,
  display_name string NOT NULL,
  department string NOT NULL,
  cost_center string,
  CONSTRAINT pk PRIMARY KEY(user_name)
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS cost_agg_day(
  user_name string NOT NULL,
  cloud string NOT NULL,
  billing_date date NOT NULL,
  account_id string NOT NULL,
  warehouse_id string NOT NULL,
  workspace_id string NOT NULL,
  dbu decimal(38, 2),
  dbu_cost decimal(38, 2),
  cloud_cost decimal(38, 2),
  currency_code string NOT NULL,
  dbu_contribution_percent decimal(17, 14),
  CONSTRAINT cost_agg_day_pk PRIMARY KEY(user_name, cloud, billing_date, account_id, warehouse_id, workspace_id),
  CONSTRAINT cost_agg_day_users_fk FOREIGN KEY (user_name) REFERENCES user_info
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS checkpoint(
  last_processed_date date NOT NULL
);

-- COMMAND ----------
