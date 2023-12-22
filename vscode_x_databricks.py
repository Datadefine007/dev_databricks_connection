# Databricks notebook source
from pyspark.sql import Row
import random

num_records = 20
data = []
departments = ["HR", "IT", "Finance", "Marketing", "Sales"]
for _ in range(num_records):
    name = f"Employee{_ + 1}"
    department = random.choice(departments)
    age = random.randint(25, 60)
    salary = random.randint(50000, 100000)
    data.append(Row(name=name, department=department, age=age, salary=salary))

df = spark.createDataFrame(data)

df.display()

# COMMAND ----------
