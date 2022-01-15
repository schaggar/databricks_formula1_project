# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion - lap_times folder (multiple csv files)

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

lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("lap", IntegerType(), False),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Read data from the folder in datalake gen2 

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f'{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Rename and add columns as per the requirement

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

lap_times_renamed_df = lap_times_df.withColumnRenamed("raceId","race_id") \
                                   .withColumnRenamed("driverId","driver_id")


# COMMAND ----------

lap_times_renamed_df = add_ingestion_date(lap_times_renamed_df).withColumn("data_source", lit(v_data_source)) \
                                                               .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(lap_times_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Write the data to datalake gend 2 processed folder as parquet

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"
merge_delta_data(lap_times_renamed_df, "f1_processed", "lap_times", "race_id", processed_folder_path, merge_condition)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.lap_times;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from f1_processed.lap_times;

# COMMAND ----------

dbutils.notebook.exit("Success")
