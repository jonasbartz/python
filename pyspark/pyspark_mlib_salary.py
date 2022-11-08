import findspark
findspark.init("C:\spark")

from pyspark.sql import SparkSession

spark = SparkSession.builder \
   .master("local") \
   .appName("Linear Regression Model") \
   .config("spark.executor.memory", "1gb") \
   .getOrCreate()

sc = spark.sparkContext

rdd = sc.textFile('pyspark/dados/Salary_Data.csv')

rdd = rdd.map(lambda line: line.split(","))

rdd.take(2)

sc.stop()