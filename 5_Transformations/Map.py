import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

spark = SparkSession.builder \
    .appName("Transformations") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

data = sc.textFile("5_Transformations/OnlineRetail.csv")
data.take(5)

data2 = data.filter(lambda x: 'InvoiceNo' not in x)
data2.take(5)


# Calculating the Total Canceled Sales Amount


