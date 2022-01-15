# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------

def rearrange_partition_column(input_df,partition_name):
    partition_column = partition_name
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_name):
    output_df = rearrange_partition_column(input_df,partition_name)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.partitionBy(partition_name).mode("overwrite").format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# include partitioned column in the join condition to get the performance and include the above configuration for partitionpruning

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, partition_name, folder_path, merge_condition):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

    from delta.tables import DeltaTable
    # include partitioned column in the join condition to get the performance and include the above configuration for partitionpruning
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt") \
          .merge(
            input_df.alias("src"),
            merge_condition) \
          .whenMatchedUpdateAll() \
          .whenNotMatchedInsertAll() \
          .execute()
    else:
        input_df.write.partitionBy(partition_name).mode("overwrite").format("delta").saveAsTable(f"{db_name}.{table_name}")
