# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

dbutils.notebook.run("1.ingest_circuit_file", 0, {"p_data_source": "Ergast API"})