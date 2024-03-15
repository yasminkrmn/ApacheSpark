import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("sql_queries") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

df = spark.read \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .option("sep", ";") \
    .csv("5_Transformations/OnlineRetail.csv")

df.show(3)
# +---------+---------+--------------------+--------+---------------+---------+----------+--------------+
# |InvoiceNo|StockCode|         Description|Quantity|    InvoiceDate|UnitPrice|CustomerID|       Country|
# +---------+---------+--------------------+--------+---------------+---------+----------+--------------+
# |   536365|   85123A|WHITE HANGING HEA...|       6|1.12.2010 08:26|     2,55|     17850|United Kingdom|
# |   536365|    71053| WHITE METAL LANTERN|       6|1.12.2010 08:26|     3,39|     17850|United Kingdom|
# |   536365|   84406B|CREAM CUPID HEART...|       8|1.12.2010 08:26|     2,75|     17850|United Kingdom|
# +---------+---------+--------------------+--------+---------------+---------+----------+--------------+
# only showing top 3 rows

df.cache()

df.createOrReplaceTempView("table")

spark.sql("""

SELECT Country, SUM(UnitPrice) UnitPrice
FROM table
GROUP BY Country
ORDER BY UnitPrice DESC


""").show(5)

# |       Country|UnitPrice|
# +--------------+---------+
# |United Kingdom|  94911.0|
# |          EIRE|   9423.0|
# |       Germany|   7930.0|
# |        France|   6288.0|
# |         Spain|   2927.0|
# +--------------+---------+
# only showing top 5 rows


