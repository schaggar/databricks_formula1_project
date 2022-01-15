-- Databricks notebook source
select file_date,count(*) from f1_processed.results group by 1

-- COMMAND ----------


