# Databricks notebook source
# from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
 
# Create a Spark session
# spark = SparkSession.builder.appName("example").getOrCreate()
 
# Define the schema for the DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("mailid", StringType(), True)
])
 
# Sample data
data = [
    ("John", 60000, 30, "john@example.com"),
    ("Alice", 75000, 35, "alice@example.com"),
    ("Bob", 80000, 28, "bob@example.com"),
    ("Charlie", 90000, 40, "charlie@example.com"),
    ("David", 65000, 32, "david@example.com"),
    ("Eve", 70000, 27, "eve@example.com"),
    ("Frank", 85000, 45, "frank@example.com"),
    ("Grace", 72000, 31, "grace@example.com"),
    ("Hank", 95000, 38, "hank@example.com"),
    ("Ivy", 68000, 29, "ivy@example.com")
]
 
# Create a PySpark DataFrame
df = spark.createDataFrame(data, schema=schema)
 
# Show the DataFrame
df.display()
print('test')


print("demi_uc_testing")
