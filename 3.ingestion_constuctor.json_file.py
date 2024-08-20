# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest the JSON File Using the Spark DataFrame reader.

# COMMAND ----------

constructor_schema= "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df= spark.read\
    .schema(constructor_schema)\
    .json("/mnt/raw/shu1/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####droping the unwanted column

# COMMAND ----------

constructor_dropped_df=constructor_df.drop("url")

# COMMAND ----------

#rename the column and add the ingestion date
from pyspark.sql.functions import *
final_constructor_df=constructor_dropped_df.select(col("constructorId").alias("constructorIid"),col("constructorRef").alias("constructor_ref"), col("name"), col("nationality"))\
    .withColumn("ingestion_date", current_date())


# COMMAND ----------

# writing the dataframe to the ADLS processed layer

final_constructor_df.write.mode("overwrite").parquet("/mnt/processed/shu1/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/processed/shu1/constructors