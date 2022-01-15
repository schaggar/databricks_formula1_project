# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion - qualifying folder (multiple json files)

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

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Read data from the folder in datalake gen2 

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f'{raw_folder_path}/{v_file_date}/qualifying/*')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Rename and add columns as per the requirement

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id") \
                                   .withColumnRenamed("raceId","race_id") \
                                   .withColumnRenamed("driverId","driver_id") \
                                   .withColumnRenamed("constructorId","constructor_id") \
                                   .withColumn("data_source", lit(v_data_source)) \
                                   .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

qualifying_renamed_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Write the data to datalake gend 2 processed folder as parquet

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_renamed_df, "f1_processed", "qualifying", "race_id", processed_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.qualifying;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from f1_processed.qualifying;

# COMMAND ----------

dbutils.notebook.exit("Success")
