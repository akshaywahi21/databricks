# Databricks notebook source
print('hello')
print(1+7)

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()

df = spark.read.csv("file:/Workspace/Users/akshaywahi@gmail.com/files/industry.csv", header=True, inferSchema=True)
display(df)
