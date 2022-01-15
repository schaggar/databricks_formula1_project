# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion - pit_stops.json (multiline json file)

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
# MAGIC #### Step 1: Define DataType

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("stop", IntegerType(), False),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Read data from datalake gen2 using multiline option from DataFrame API

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f'{raw_folder_path}/{v_file_date}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Rename and add columns as per the requirement

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pit_stops_renamed_df = pit_stops_df.withColumnRenamed("raceId","race_id") \
                                   .withColumnRenamed("driverId","driver_id") 

# COMMAND ----------

pit_stops_renamed_df = add_ingestion_date(pit_stops_renamed_df).withColumn("data_source",lit(v_data_source)) \
                                                               .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Write the data to datalake gend 2 processed folder as parquet

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_data(pit_stops_renamed_df, "f1_processed", "pit_stops", "race_id", processed_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from f1_processed.pit_stops;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pit_stops;

# COMMAND ----------

dbutils.notebook.exit("Success")
