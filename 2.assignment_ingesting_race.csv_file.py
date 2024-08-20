# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

schema_race=StructType(fields=[StructField("raceId", IntegerType() , True),
                               StructField("year",IntegerType() , True),
                               StructField("round", IntegerType() , True),
                               StructField("circuitId", IntegerType() , True),
                               StructField("name", StringType() , True),
                               StructField("date", DateType() , True),
                               StructField("time", StringType() , True),
                               StructField("url", StringType() , True)])

# COMMAND ----------

race_df=spark.read.format("csv")\
    .option("header", True)\
    .schema(schema_race)\
    .load("dbfs:/mnt/raw/shu1/races.csv")

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

# # add ingestion_date and race_timestamp to the dataframe

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit


race_with_timestamp_df = race_df.withColumn("ingestion_time", current_timestamp()) \
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss'))

# display(race_with_timestamp_df)
race_with_timestamp_df.printSchema()

# COMMAND ----------

# renaming the column and select the required column
race_selected_df=race_with_timestamp_df.select(col("raceId").alias("race_id"),col("round"),col("circuitId").alias("circuit_id"), col("year").alias("race_year"), col("name"), col("ingestion_time"), col("race_timestamp"))


# COMMAND ----------

race_selected_df.write.mode("overwrite").parquet("/mnt/processed/shu1/race")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/shu1/processed/race

# COMMAND ----------

spark.read.parquet("/mnt/shu1/processed/race")



# COMMAND ----------

