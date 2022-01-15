# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion - Results.json

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Reading data from datalake using DataFrame reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 2: Renaming and adding new columns to the dataframe

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId","result_id") \
                                .withColumnRenamed("raceId","race_id") \
                                .withColumnRenamed("driverId","driver_id") \
                                .withColumnRenamed("constructorId","constructor_id") \
                                .withColumnRenamed("positionText","position_text") \
                                .withColumnRenamed("positionOrder","position_order") \
                                .withColumnRenamed("fastestLap","fastest_lap") \
                                .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                                .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
                                .drop(col("statusId")) \
                                .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

results_renamed_df = add_ingestion_date(results_renamed_df).withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Deduped dataframe

# COMMAND ----------

results_deduped_df = results_renamed_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Writing the data to datalake gen 2 and partitioning using colunm race_id

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id and tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, "f1_processed", "results", "race_id", processed_folder_path, merge_condition)

# COMMAND ----------

#overwrite_partition(results_renamed_df,'f1_processed','results','race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select file_date,count(*) from f1_processed.results group by 1;

# COMMAND ----------

dbutils.notebook.exit("Success")
