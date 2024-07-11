-- Databricks notebook source

-- COMMAND ----------

USE CATALOG ${catalog};

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${schema};

-- COMMAND ----------

USE SCHEMA ${schema};

-- COMMAND ----------

-- remove data older than 3 months
DELETE FROM user_costs_day WHERE billing_date < DATE_SUB(CURRENT_DATE(), 90)
-- COMMAND ----------

-- remove data older than 3 months
DELETE FROM user_costs_month WHERE billing_date < DATE_SUB(CURRENT_DATE(), 90)
