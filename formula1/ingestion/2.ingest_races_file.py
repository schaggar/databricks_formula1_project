# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion - races.csv

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Read the file from raw folder

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", StringType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
                                  
    
])

# COMMAND ----------

races_df = spark.read \
.option("header",True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Rename the columns

# COMMAND ----------

from pyspark.sql.functions import col,lit,to_timestamp, concat, current_timestamp

# COMMAND ----------

races_selected_df = races_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"),col("round"), col("circuitId").alias("circuit_id"),col("name"),col("date"),col("time"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Concatenate the column

# COMMAND ----------

races_new_col_df = races_selected_df.withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Add the column

# COMMAND ----------

races_final_df = races_new_col_df.drop("date").drop("time")

# COMMAND ----------

races_final_df = add_ingestion_date(races_final_df)

# COMMAND ----------

races_final_df = races_final_df.withColumn("data_source",lit(v_data_source)) \
                                .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Write the data to the datalake gen 2

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Command to convert parquet to delta in case of partitioned table

# COMMAND ----------

# %sql
# CONVERT TO DELTA f1_processed.races PARTITIONED BY (race_year int);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")
