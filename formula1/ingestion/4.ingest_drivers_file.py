# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion - drivers.json (nested)

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
# MAGIC ### Step 1: Read the json from datalake gen 2 raw folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Defining the schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                 StructField("driverRef", StringType(), True),
                                 StructField("number", IntegerType(), True),
                                 StructField("code", StringType(), True),
                                 StructField("name", name_schema),
                                 StructField("dob", DateType(), True),
                                 StructField("nationality", StringType(), True),
                                 StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
                .schema(drivers_schema) \
                .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col,concat

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Transform and Rename

# COMMAND ----------

drivers_transform_df = drivers_df.withColumnRenamed("driverId","driver_id") \
                                 .withColumnRenamed("driverRef","driver_ref") \
                                 .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname"))) \
                                 .drop(col("url")) \
                                 .withColumn("data_source",lit(v_data_source)) \
                                 .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

drivers_transform_df = add_ingestion_date(drivers_transform_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Writing the data to datalake gen 2

# COMMAND ----------

drivers_transform_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# %sql
# convert to delta f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")
