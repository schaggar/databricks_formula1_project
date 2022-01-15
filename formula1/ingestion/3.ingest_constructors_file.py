# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion - Consturctors.json file (single line json)

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
# MAGIC ### Step 1: Reading json file using dataframe reader API

# COMMAND ----------

#This is another way of defining the schema other than using StructType and StructField, however we can use the same here as well
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
.schema(constructors_schema) \
.json(f'{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Renaming and adding columns

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp, lit

# COMMAND ----------

constructors_selected_df = constructors_df.select(col("constructorId").alias("constructor_id"),col("constructorRef").alias("constructor_ref"),col("name"),col("nationality"))

# COMMAND ----------

constructors_selected_df = add_ingestion_date(constructors_selected_df).withColumn("data_source",lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Writing the data to datalake gen 2 as parquet

# COMMAND ----------

constructors_selected_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# %sql
# convert to delta f1_processed.constructors;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")
