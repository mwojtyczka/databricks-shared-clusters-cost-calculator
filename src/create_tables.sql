-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Creating Tables

-- COMMAND ----------

USE CATALOG ${catalog};

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
  cost_center int,
  CONSTRAINT pk PRIMARY KEY(user_name)
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_costs_day(
  user_name string NOT NULL,
  cloud string NOT NULL,
  billing_date date NOT NULL,
  account_id string NOT NULL,
  warehouse_id string NOT NULL,
  workspace_id string NOT NULL,
  dbu decimal(38, 2) NOT NULL,
  dbu_cost decimal(38, 2) NOT NULL,
  cloud_cost decimal(38, 2) NOT NULL,
  currency_code string NOT NULL,
  dbu_contribution_percent decimal(17, 14) NOT NULL,
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
  CONSTRAINT user_costs_day_pk PRIMARY KEY(user_name, cloud, billing_date, account_id, warehouse_id, workspace_id),
  CONSTRAINT user_costs_day_users_fk FOREIGN KEY (user_name) REFERENCES user_info
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_costs_month(
  user_name string NOT NULL,
  cloud string NOT NULL,
  billing_date date NOT NULL, -- always as 1st day of the month
  billing_year int NOT NULL,
  billing_month int NOT NULL,
  account_id string NOT NULL,
  workspace_id string NOT NULL,
  dbu decimal(38, 2) NOT NULL,
  dbu_cost decimal(38, 2) NOT NULL,
  cloud_cost decimal(38, 2) NOT NULL,
  currency_code string NOT NULL,
  dbu_contribution_percent decimal(17, 14) NOT NULL,
  CONSTRAINT user_costs_month_pk PRIMARY KEY(user_name, billing_year, billing_month, account_id, workspace_id),
  CONSTRAINT user_costs_month_users_fk FOREIGN KEY (user_name) REFERENCES user_info
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS checkpoint_day(
  last_processed_date date NOT NULL
);

CREATE TABLE IF NOT EXISTS checkpoint_month(
  last_processed_date date NOT NULL
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Preparing test data

-- COMMAND ----------

-- TODO testing only!
INSERT INTO user_info(user_name, user_id, display_name, department, cost_center)
VALUES ("marcin.wojtyczka@databricks.com", "1", "Marcin Wojtyczka", "PS", 1),
       ("felix.mutzl@databricks.com", "2", "Felix Mutzl", "FE", 2)
