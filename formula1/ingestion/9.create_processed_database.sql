-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/cdldevadl2eussch/processed"

-- COMMAND ----------

describe database extended f1_processed;

-- COMMAND ----------

describe database extended f1_raw;

-- COMMAND ----------

drop database f1_processed;

-- COMMAND ----------


