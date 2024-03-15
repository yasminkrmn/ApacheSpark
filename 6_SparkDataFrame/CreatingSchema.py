import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .appName("CreatingSchema") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext



df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ";") \
    .csv("5_Transformations/OnlineRetail.csv") \
    .withColumn("UnitPrice", F.regexp_replace(F.col("UnitPrice"), ",", "."))

df.show(5)

# +---------+---------+--------------------+--------+---------------+---------+----------+--------------+
# |InvoiceNo|StockCode|         Description|Quantity|    InvoiceDate|UnitPrice|CustomerID|       Country|
# +---------+---------+--------------------+--------+---------------+---------+----------+--------------+
# |   536365|   85123A|WHITE HANGING HEA...|       6|1.12.2010 08:26|     2.55|     17850|United Kingdom|
# |   536365|    71053| WHITE METAL LANTERN|       6|1.12.2010 08:26|     3.39|     17850|United Kingdom|
# |   536365|   84406B|CREAM CUPID HEART...|       8|1.12.2010 08:26|     2.75|     17850|United Kingdom|
# |   536365|   84029G|KNITTED UNION FLA...|       6|1.12.2010 08:26|     3.39|     17850|United Kingdom|
# |   536365|   84029E|RED WOOLLY HOTTIE...|       6|1.12.2010 08:26|     3.39|     17850|United Kingdom|
# +---------+---------+--------------------+--------+---------------+---------+----------+--------------+
# only showing top 5 rows

df.printSchema()
# root
#  |-- InvoiceNo: string (nullable = true)
#  |-- StockCode: string (nullable = true)
#  |-- Description: string (nullable = true)
#  |-- Quantity: integer (nullable = true)
#  |-- InvoiceDate: string (nullable = true)
#  |-- UnitPrice: string (nullable = true)
#  |-- CustomerID: integer (nullable = true)
#  |-- Country: string (nullable = true)



# Writing to Disk:
df \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "True") \
    .csv("6_SparkDataFrame/retail")

# Creating Schema:

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

schema = StructType(
    [
     StructField("InvoiceNo", StringType(), True),
     StructField("StockCode", StringType(), True),
     StructField("Description", StringType(), True),
     StructField("Quantity", IntegerType(), True),
     StructField("InvoiceDate", StringType(), True),
     StructField("UnitPrice", FloatType(), True),
     StructField("CustomerID", IntegerType(), True),
     StructField("Country", StringType(), True)
    ]
)

df2 = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .option("sep", ",") \
    .csv("6_SparkDataFrame/online_retail_2.csv")

df2.show(5)
# +---------+---------+--------------------+--------+---------------+---------+----------+--------------+
# |InvoiceNo|StockCode|         Description|Quantity|    InvoiceDate|UnitPrice|CustomerID|       Country|
# +---------+---------+--------------------+--------+---------------+---------+----------+--------------+
# |   536365|   85123A|WHITE HANGING HEA...|       6|1.12.2010 08:26|     2.55|     17850|United Kingdom|
# |   536365|    71053| WHITE METAL LANTERN|       6|1.12.2010 08:26|     3.39|     17850|United Kingdom|
# |   536365|   84406B|CREAM CUPID HEART...|       8|1.12.2010 08:26|     2.75|     17850|United Kingdom|
# |   536365|   84029G|KNITTED UNION FLA...|       6|1.12.2010 08:26|     3.39|     17850|United Kingdom|
# |   536365|   84029E|RED WOOLLY HOTTIE...|       6|1.12.2010 08:26|     3.39|     17850|United Kingdom|
# +---------+---------+--------------------+--------+---------------+---------+----------+--------------+
# only showing top 5 rows

df2.printSchema()
# root
#  |-- InvoiceNo: string (nullable = true)
#  |-- StockCode: string (nullable = true)
#  |-- Description: string (nullable = true)
#  |-- Quantity: integer (nullable = true)
#  |-- InvoiceDate: string (nullable = true)
#  |-- UnitPrice: float (nullable = true)
#  |-- CustomerID: integer (nullable = true)
#  |-- Country: string (nullable = true)



