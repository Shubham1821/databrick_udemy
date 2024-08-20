# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest Circuit.csv file

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step1. Read the csv file using spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/raw/shu1

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

circuit_schema=StructType(fields=[StructField("circuitId", IntegerType(),  False),
                                 StructField ("circuitRef", StringType(),  True), 
                                StructField ("name", StringType(),  True),
                                StructField("location", StringType(),  True),
                                StructField("country", StringType(),  True),
                                StructField("lat", DoubleType(),  True),
                                StructField( "lng", DoubleType(),  True),
                                StructField("alt", IntegerType(),  True),
                                StructField("url", StringType(),  True)])

# COMMAND ----------

circuit_df=spark.read.format("csv")\
       .option("header", "True")\
       .schema(circuit_schema)\
       .load(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only the required column

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

selected_circuit_df= circuit_df.select(col("circuitID"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# Rename the columns as required

circuit_renamed_df=selected_circuit_df.withColumnRenamed("circuitId","circuit_Id")\
    .withColumnRenamed("circuitRef","circuit_ref")\
    .withColumnRenamed("lat","latitude")\
    .withColumnRenamed("lng","longitude")\
    .withColumnRenamed("alt","altitude")\
    .withColumn("source_data", lit(v_data_source))

# COMMAND ----------

# Add new column(ingestion timestamp) to the dataframe

circuit_final_df= add_ingestion_date(circuit_renamed_df)



# COMMAND ----------

# Write the data to the ADLS in Parguet
circuit_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/processed/circuits

# COMMAND ----------

df=spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------

