# Databricks notebook source
# MAGIC %md
# MAGIC # Presentation Layer Data Set Creation
# MAGIC * Dataset used - races, circuits, drivers, constructors, results

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, to_date, current_timestamp

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races").select(col("circuit_id"),col("race_id"),col("race_year"),col("name").alias("race_name"),to_date("race_timestamp", "yyyy-MM-dd").alias("race_date"))

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits").select(col("circuit_id"),col("location").alias("circuit_location"))

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
.select(col("result_id"),col("race_id"),col("driver_id"),col("constructor_id"),col("grid"),col("fastest_lap"),col("time").alias("race_time"),col("points"),col("position") \
,col("file_date").alias("results_file_date")) \
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

results_df

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
.select(col("driver_id"),col("number").alias("driver_number"),col("name").alias("driver_name"),col("nationality").alias("driver_nationality"))

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors").select(col("constructor_id"),col("name").alias("team"))

# COMMAND ----------

df1 = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df["circuit_id"],races_df["race_id"],col("race_year"),col("race_name"),col("race_date"),col("circuit_location"))

# COMMAND ----------

df2 = df1.join(results_df, df1.race_id == results_df.race_id, "inner") \
.select(col("circuit_id"),df1["race_id"],col("driver_id"),col("constructor_id"),col("result_id"),col("race_year"), col("race_name"), col("race_date"), col("circuit_location"), \
        col("grid"),col("fastest_lap"),col("race_time"),col("points"),col("position"),col("results_file_date").alias("file_date"))

# COMMAND ----------

df3 = df2.join(drivers_df, df2.driver_id == drivers_df.driver_id, "inner") \
.select(col("circuit_id"),df2["race_id"],df2["driver_id"],col("constructor_id"),col("result_id"),col("race_year"), col("race_name"), col("race_date"), col("circuit_location"), \
        col("grid"),col("fastest_lap"),col("race_time"),col("points"),col("position"),col("driver_number"),col("driver_name"),col("driver_nationality"),col("file_date"))

# COMMAND ----------

final_df = df3.join(constructors_df, constructors_df.constructor_id == df3.constructor_id, "inner") \
.select(col("circuit_id"),df3["race_id"],df3["driver_id"],df3["constructor_id"],col("result_id"),col("race_year"), col("race_name"), col("race_date"), col("circuit_location"),\
        col("grid"),col("fastest_lap"),col("race_time"),col("points"),col("position"),col("driver_number"),col("driver_name"),col("driver_nationality"),col("team"),col("file_date")).withColumn("created_date",current_timestamp())

# COMMAND ----------

#overwrite_partition(final_df,'f1_presentation','race_results','race_id')

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name and tgt.race_id = src.race_id"
merge_delta_data(final_df, "f1_presentation", "race_results", "race_id", presentation_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,race_id,count(*) from f1_presentation.race_results group by 1,2 order by race_id desc;

# COMMAND ----------

dbutils.notebook.exit("Success")
