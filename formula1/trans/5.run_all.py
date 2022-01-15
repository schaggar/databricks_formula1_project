# Databricks notebook source
dbutils.notebook.run("1.race_results",0,{"p_file_date":"2021-03-21"})
dbutils.notebook.run("1.race_results",0,{"p_file_date":"2021-03-28"})
dbutils.notebook.run("1.race_results",0,{"p_file_date":"2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("2.driver_standings",0,{"p_file_date":"2021-03-21"})
dbutils.notebook.run("2.driver_standings",0,{"p_file_date":"2021-03-28"})
dbutils.notebook.run("2.driver_standings",0,{"p_file_date":"2021-04-14"})



# COMMAND ----------

dbutils.notebook.run("3.constructor_standings",0,{"p_file_date":"2021-03-21"})
dbutils.notebook.run("3.constructor_standings",0,{"p_file_date":"2021-03-28"})
dbutils.notebook.run("3.constructor_standings",0,{"p_file_date":"2021-04-18"})

# COMMAND ----------


