-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### (Re)-deploy Budget

-- COMMAND ----------

USE ${output_catalog}.${output_schema};

-- COMMAND ----------

DELETE FROM ${output_catalog}.${output_schema}.budget;

INSERT INTO ${output_catalog}.${output_schema}.budget(organizational_entity_name, organizational_entity_value, dbu_cost_limit, cloud_cost_limit, currency_code, effective_start_date, effective_end_date) VALUES
("department", "R&D", 3000000.0, 3500000.0, "USD", "2024-04-01", NULL),
("department", "FE", 3000000.0, 3500000.0, "USD", "2024-04-01", NULL),
("department", "PS", 1000000.0, 1500000.0, "USD", "2024-04-01", NULL);

-- COMMAND ----------
