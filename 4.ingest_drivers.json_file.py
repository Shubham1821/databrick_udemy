# Databricks notebook source
# Read the json file using dataframe reader API
from pyspark.sql.types import *

name_schema=StructType(fields=[StructField("forename", StringType(), True),StructField("surname", StringType(), True)])

drivers_schema= StructType(fields=[
                                StructField("driverId", IntegerType(),True), 
                                StructField("driverRef", StringType(), True),
                                StructField("number", IntegerType(), True),
                                StructField("name", name_schema),
                                StructField("dob", DateType(), True),
                                StructField("code", StringType(),False),
                                StructField( "nationality", StringType(), True),
                                StructField("url", StringType(),True)])
driver_df=spark.read.option("header", True)\
    .schema(drivers_schema)\
    .json("/mnt/raw/shu1/drivers.json")


# COMMAND ----------

# Rename column  and new column to the dataframe
from pyspark.sql.functions import *
driver_with_columns_df= driver_df.withColumnRenamed("driverId", "driver_id")\
                                 .withColumnRenamed("driverRef", "driver_ref")\
                                 .withColumn("ingestion_date", current_timestamp())\
                                 .withColumn("name", concat(col("name.forename"), lit(" "),col("name.surname")))

# COMMAND ----------

# Drop the unwanted column
driver_final_df= driver_with_columns_df.drop(col("url"))

# COMMAND ----------

# Write the output to the proceesed layer in parquet format
driver_final_df.write.mode("overwrite").parquet("/mnt/processed/shu1/drivers")