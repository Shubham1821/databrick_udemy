# Databricks notebook source
# reading the file from dataframe api

from pyspark.sql.types import *


# COMMAND ----------

qualifying_schema= StructType(fields=[StructField("qualifyId", IntegerType(), True),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("q1", StringType(), True),
                                    StructField("q2", StringType(), True),
                                    StructField("q3", StringType(), True)])

# COMMAND ----------

qualify_df=spark.read.option("multiLine", True)\
               .schema(qualifying_schema)\
               .json("/mnt/raw/shu1/qualifying")

# COMMAND ----------

# rename and add the column to the dataframe
from pyspark.sql.functions import *

final_df=qualify_df.withColumnRenamed("qualifyId","qualify_id")\
                    .withColumnRenamed("raceId", "race_id")\
                    .withColumnRenamed("driverId", "driver_id")\
                    .withColumnRenamed("constructorId","constructor_Id")\
                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/processed/shu1/qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/shu1/qualifying"))