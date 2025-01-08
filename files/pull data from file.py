# Databricks notebook source
print('hello')
print(1+7)

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()

df = spark.read.csv("file:/Workspace/Users/akshaywahi@gmail.com/files/industry.csv", header=True, inferSchema=True)
display(df)

# COMMAND ----------

sfOptions = {
  "sfURL" : "vq97794.central-india.azure.snowflakecomputing.com",
  "sfDatabase" : "SNOWFLAKE_SAMPLE_DATA",
  "sfSchema" : "TPCDS_SF100TCL",
  "sfWarehouse" : "XSWH",
  "sfRole" : "SYSADMIN",  # Optional
  "sfUser" : "awahi",
  "sfPassword" : "Bhai@1990"
}

df = spark.read.format("snowflake") \
    .options(**sfOptions) \
    .option("query", "SELECT * FROM CATALOG_PAGE LIMIT 10") \
    .load()
## df to show data
display(df)
