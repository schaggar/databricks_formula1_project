# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") 

# COMMAND ----------

display(races_df)

# COMMAND ----------

#sql way
display(races_df.filter("race_year = 2019 and round <=5"))

# COMMAND ----------

#sql way using where
display(races_df.where("race_year = 2019 and round <=5"))

# COMMAND ----------

#Python way
display(races_df.filter(races_df["race_year"] == 2019))

# COMMAND ----------


