# Databricks notebook source
spark.read.json("/mnt/raw/shu1/results.json").printSchema()


# COMMAND ----------

# read the json file from dataframe api
from pyspark.sql.types import *

results_schema= StructType(fields=[StructField("resultId", IntegerType(),False),
                                  StructField("raceId", IntegerType(), True),
                                  StructField("constructorId", IntegerType(), True),
                                   StructField("driverId", IntegerType(), True),
                                   StructField("grid", IntegerType(), True),
                                   StructField("laps", IntegerType(),  True),
                                   StructField("milliseconds", IntegerType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("points", FloatType(), True ),
                                   StructField("position", IntegerType(), True ),
                                    StructField("rank", IntegerType(), True),
                                   StructField("statusId", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("fastestLap", IntegerType(), True ),
                                   StructField("fastestLapSpeed", FloatType(), True ),
                                   StructField("fastestLapTime", StringType(), True),
                                   StructField("positionOrder", IntegerType(), True),
                                   StructField("positionText", StringType(), True)])



# COMMAND ----------

results_df= spark.read.option("header", True)\
                    .schema(results_schema)\
                    .json("/mnt/raw/shu1/results.json")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------


display(results_df)

# COMMAND ----------

# rename and add the column to dataframe
from pyspark.sql.functions import *
results_with_column_df = results_df.withColumnRenamed("resultId", "result_id")\
                                    .withColumnRenamed("raceId", "race_id" )\
                                    .withColumnRenamed("constructorId", "constructor_id")\
                                    .withColumnRenamed("driverId","driver_id")\
                                    .withColumnRenamed("statusId", "status_id")\
                                    .withColumnRenamed("fastestLap","fastest_lap" )\
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed" )\
                                    .withColumnRenamed("fastestLapTime", "fasted_lap_time")\
                                    .withColumnRenamed("positionOrder", "position_order")\
                                    .withColumnRenamed("positionText", "position_text")\
                                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# drop the unwanted column from dataframe
results_final_df=results_with_column_df.drop(col("status_id"))

# COMMAND ----------

# write the output to processed layer in parquet format
results_final_df.write.mode("overwrite").parquet("/mnt/processed/shu1/results")