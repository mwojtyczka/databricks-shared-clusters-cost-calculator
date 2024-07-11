-- Databricks notebook source

-- COMMAND ----------

USE CATALOG ${catalog};

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${schema};

-- COMMAND ----------

USE SCHEMA ${schema};

-- COMMAND ----------

DELETE FROM user_costs_day WHERE user_name = '${user_name}';
DELETE FROM user_costs_month WHERE user_name = '${user_name}';
DELETE FROM user_info WHERE user_name = '${user_name}';
