-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Creating Tables

-- COMMAND ----------

USE CATALOG ${catalog};

-- COMMAND ----------

-- TODO testing only! remove once code is ready
DROP SCHEMA ${schema} cascade

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${schema};

-- COMMAND ----------

USE SCHEMA ${schema};

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
  total_duration_ms long,
  execution_duration_ms long,
  compilation_duration_ms long,
  total_task_duration_ms long,
  result_fetch_duration_ms long,
  read_partitions long,
  pruned_files long,
  read_files long,
  read_rows long,
  produced_rows long,
  read_bytes long,
  spilled_local_bytes long,
  written_bytes long,
  shuffle_read_bytes long,
  CONSTRAINT cost_agg_day_pk PRIMARY KEY(user_name, cloud, billing_date, account_id, warehouse_id, workspace_id),
  CONSTRAINT cost_agg_day_users_fk FOREIGN KEY (user_name) REFERENCES user_info
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS cost_agg_month(
  user_name string NOT NULL,
  cloud string NOT NULL,
  billing_date date NOT NULL, -- always as 1st day of the month
  billing_year int NOT NULL,
  billing_month int NOT NULL,
  account_id string NOT NULL,
  workspace_id string NOT NULL,
  warehouse_id string NOT NULL,
  dbu decimal(38, 2) NOT NULL,
  dbu_cost decimal(38, 2) NOT NULL,
  cloud_cost decimal(38, 2),
  currency_code string NOT NULL,
  dbu_contribution_percent decimal(17, 14) NOT NULL,
  CONSTRAINT cost_agg_month_pk PRIMARY KEY(user_name, billing_year, billing_month, account_id, workspace_id),
  CONSTRAINT cost_agg_month_users_fk FOREIGN KEY (user_name) REFERENCES user_info
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS checkpoint_day(
  last_processed_date date NOT NULL
);

CREATE TABLE IF NOT EXISTS checkpoint_month(
  last_processed_date date NOT NULL
);

-- COMMAND ----------
