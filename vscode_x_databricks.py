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







dem_task=[]
for i in range(1,111):
    dem_task.append(i)
d2=1
for i in dem_task:
    d2*=i
print(d2)
    




# Upcoming Dataframe
from pyspark.sql import SparkSession
from pyspark.sql import Row
import random

num_records = 20

data = []
classes = ["Class 1", "Class 2", "Class 3", "Class 4", "Class 5"]
locations = ["Delhi", "Mumbai", "Chennai", "Kolkata", "Bangalore"]

for _ in range(num_records):
    name = f"Student{_ + 1}"
    class_name = random.choice(classes)
    score = round(random.uniform(50, 100), 2) 
    attendance_rate = round(random.uniform(70, 100), 2)  
    location = random.choice(locations)

    data.append(Row(name=name, class=class_name, score=score, attendance_rate=attendance_rate, location=location))

df = spark.createDataFrame(data)

df.display()
