# My First PySpark Program

import os

from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, schema_of_json

os.environ[
    "PYSPARK_PYTHON"] = "'C:/Users/DellX/AppData/Local/Programs/Python/Python37/python.exe"

# Create Spark Session
spark = SparkSession.builder \
    .appName("Example") \
    .master("local[*") \
    .getOrCreate()

schema="id int,Name string,Age int,Salary int,city string,details int, mean int"

df = spark.read \
    .format("csv") \
    .option("header",True) \
    .schema(schema) \
    .option("path", "C:/Users/DellX/Documents/data/info.csv") \
    .load()

df.show()
df.printSchema()