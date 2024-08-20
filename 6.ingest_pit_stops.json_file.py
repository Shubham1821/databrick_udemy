# Databricks notebook source
# Read the file through dataframe api
from pyspark.sql.types import *

# COMMAND ----------

pitstops_schema= StructType(fields=[StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("stop", StringType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("duration", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

pit_stops_df=spark.read.option("multiLine", True)\
         .schema(pitstops_schema)\
         .json("/mnt/raw/shu1/pit_stops.json")

# COMMAND ----------

# rename and add column to the dataframe
from pyspark.sql.functions import *
pit_stops_final_df= pit_stops_df.withColumnRenamed("raceId", "race_id")\
                                    .withColumnRenamed("driverid", "driver_id")\
                                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# write the output to the processed layer in parquet format.
pit_stops_final_df.write.mode("overwrite").parquet("/mnt/processed/shu1/pitstops")