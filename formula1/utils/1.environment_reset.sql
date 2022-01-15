-- Databricks notebook source
DROP DATABASE f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE f1_processed
LOCATION '/mnt/cdldevadl2eussch/processed'

-- COMMAND ----------

DROP DATABASE f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE f1_presentation
LOCATION '/mnt/cdldevadl2eussch/presentation'
