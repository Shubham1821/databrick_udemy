# Databricks notebook source
# Read the file through dataframe api
from pyspark.sql.types import *

# COMMAND ----------

laptimes_schema= StructType(fields=[StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("millisecond", IntegerType(), True)])

# COMMAND ----------

lap_times_df=spark.read\
         .schema(laptimes_schema)\
         .csv("/mnt/raw/shu1/time_laps*")

# COMMAND ----------

# rename and add column to the dataframe
from pyspark.sql.functions import *
lap_times_final_df= lap_times_df.withColumnRenamed("raceId", "race_id")\
                                    .withColumnRenamed("driverid", "driver_id")\
                                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# write the output to the processed layer in parquet format.
lap_times_final_df.write.mode("overwrite").parquet("/mnt/processed/shu1/lap_times")