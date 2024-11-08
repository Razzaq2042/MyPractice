import os

# from pyspark.shell import spark
# from pyspark.sql import SparkSession
from pyspark import SparkContext

# from pyspark.sql.functions import col, when, avg, schema_of_json

os.environ["PYSPARK_PYTHON"] = "C:/Users/DellX/AppData/Local/Programs/Python/Python37/python.exe"

sc = SparkContext("local[4]", "sparkrdd")

# rdd1 = sc.textFile("C:/Users/DellX/Desktop/ForPractice/data.txt")
# rdd2 = rdd1.flatMap(lambda x:x.split(" "))
# rdd3 = rdd2.map(lambda x: (x, 1))
# rdd4 = rdd3.reduceByKey(lambda x, y: x +y)
# rdd5 = rdd4.sortBy(lambda x:x[1], False)
#
# for i in rdd5.collect():
#     print(i)

# arr = [10,20,30,40,50,60,70,80,91]
#
# rdd1=sc.parallelize(arr)
# avg=rdd1.mean()
# sum=rdd1.reduce(lambda x , y : x+y)
# c=rdd1.count()
# avg=sum/c
# print(avg)

 rdd=sc.parallelize(Array(1,2,3,4,5))
 rdd.saveAsTextFile("C:/Users/DellX/Desktop/ForPractice/avg")
