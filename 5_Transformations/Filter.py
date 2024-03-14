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

header = data.first()
header = sc.parallelize(header)
data2 = data.subtract(header)
data2.take(5)

# Filtering: Quantity > 30
data2.filter(lambda x: int(x.split(";")[3]) > 30).take(5)

# Filtering: Decription ==  "COFFEE"
data2.filter(lambda x: "COFFEE" in x.split(";")[2]).take(5)

# Filtering: Multiple Filtering using Function

def multiple_filtering(x):
    quantity = int(x.split(";")[3])
    description = x.split(";")[2]

    return (quantity > 30) and ("COFFEE" in description)

data2.filter(lambda x: multiple_filtering(x)).take(10)




