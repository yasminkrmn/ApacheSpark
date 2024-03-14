import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IntroductionDataframe") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

############### CREATING DATAFRAME ###############

# Using RDD List:

from pyspark import Row

list = sc.parallelize([1, 2, 3, 4, 5, 6, 1, 2]) \
    .map(lambda x: Row(x))
list
# Out[8]: PythonRDD[1] at RDD at PythonRDD.scala:53

df_list = list.toDF(["Numbers"])
df_list.show()

# |Numbers|
# +-------+
# |      1|
# |      2|
# |      3|
# |      4|
# |      5|
# |      6|
# |      1|
# |      2|
# +-------+

# Using Range:

range_RDD = sc.parallelize(range(3, 30, 4)).map(lambda x: (x,)) \
    .toDF(["Numbers"])
range_RDD.show()

# |Numbers|
# +-------+
# |      3|
# |      7|
# |     11|
# |     15|
# |     19|
# |     23|
# |     27|
# +-------+


# Using schema

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data2 = [("James", "", "Smith", "36636", "M", 3000),
         ("Michael", "Rose", "", "40288", "M", 4000),
         ("Robert", "", "Williams", "42114", "M", 4000),
         ("Maria", "Anne", "Jones", "39192", "F", 4000),
         ("Jen", "Mary", "Brown", "", "F", -1)
         ]

schema = StructType([ \
    StructField("firstname", StringType(), True), \
    StructField("middlename", StringType(), True), \
    StructField("lastname", StringType(), True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
    ])

df = spark.createDataFrame(data=data2, schema=schema)

df.show(truncate=False)
# |firstname|middlename|lastname|id   |gender|salary|
# +---------+----------+--------+-----+------+------+
# |James    |          |Smith   |36636|M     |3000  |
# |Michael  |Rose      |        |40288|M     |4000  |
# |Robert   |          |Williams|42114|M     |4000  |
# |Maria    |Anne      |Jones   |39192|F     |4000  |
# |Jen      |Mary      |Brown   |     |F     |-1    |
# +---------+----------+--------+-----+------+------+

df.printSchema()
# root
#  |-- firstname: string (nullable = true)
#  |-- middlename: string (nullable = true)
#  |-- lastname: string (nullable = true)
#  |-- id: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- salary: integer (nullable = true)


# From Data Source:

df = spark.read \
    .option("header", "True") \
    .option("sep", ";") \
    .option("inferSchema", "True") \
    .csv("5_Transformations/OnlineRetail.csv")

df.show(5)

# +---------+---------+--------------------+--------+---------------+---------+----------+--------------+
# |InvoiceNo|StockCode|         Description|Quantity|    InvoiceDate|UnitPrice|CustomerID|       Country|
# +---------+---------+--------------------+--------+---------------+---------+----------+--------------+
# |   536365|   85123A|WHITE HANGING HEA...|       6|1.12.2010 08:26|     2,55|     17850|United Kingdom|
# |   536365|    71053| WHITE METAL LANTERN|       6|1.12.2010 08:26|     3,39|     17850|United Kingdom|
# |   536365|   84406B|CREAM CUPID HEART...|       8|1.12.2010 08:26|     2,75|     17850|United Kingdom|
# |   536365|   84029G|KNITTED UNION FLA...|       6|1.12.2010 08:26|     3,39|     17850|United Kingdom|
# |   536365|   84029E|RED WOOLLY HOTTIE...|       6|1.12.2010 08:26|     3,39|     17850|United Kingdom|
# +---------+---------+--------------------+--------+---------------+---------+----------+--------------+

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

df.select("InvoiceNo", "StockCode").show(3)
# +---------+---------+
# |InvoiceNo|StockCode|
# +---------+---------+
# |   536365|   85123A|
# |   536365|    71053|
# |   536365|   84406B|
# +---------+---------+
# only showing top 3 rows

df.sort("UnitPrice").show(3)
# +---------+---------+---------------+--------+----------------+---------+----------+--------------+
# |InvoiceNo|StockCode|    Description|Quantity|     InvoiceDate|UnitPrice|CustomerID|       Country|
# +---------+---------+---------------+--------+----------------+---------+----------+--------------+
# |  A563186|        B|Adjust bad debt|       1|12.08.2011 14:51|-11062,06|         0|United Kingdom|
# |  A563187|        B|Adjust bad debt|       1|12.08.2011 14:52|-11062,06|         0|United Kingdom|
# |   536545|    21134|         000000|       1| 1.12.2010 14:32|        0|         0|United Kingdom|
# +---------+---------+---------------+--------+----------------+---------+----------+--------------+
# only showing top 3 rows