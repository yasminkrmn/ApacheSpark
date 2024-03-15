import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Date_Time_Operations") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

df = spark \
    .read \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .option("sep", ",") \
    .csv('6_SparkDataFrame/online_retail_2.csv')

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
#  |-- UnitPrice: double (nullable = true)
#  |-- CustomerID: integer (nullable = true)
#  |-- Country: string (nullable = true)

# Date - Time Operations:
from pyspark.sql import functions as F

df2 = df.select("InvoiceDate").distinct()
df2.show(5)

# |     InvoiceDate|
# +----------------+
# | 3.12.2010 16:50|
# | 7.12.2010 12:28|
# | 8.12.2010 15:02|
# |10.12.2010 09:53|
# |12.12.2010 13:32|
# +----------------+
# only showing top 5 rows


current_format = "d.MM.yyyy HH:mm"

df_ = df2 \
    .withColumn("Date", F.to_date(F.col("InvoiceDate"), current_format)) \
    .withColumn("TimeStamp", F.to_timestamp(F.col("InvoiceDate"), current_format))

df_.show(5)

# +----------------+----------+-------------------+
# |     InvoiceDate|      Date|          TimeStamp|
# +----------------+----------+-------------------+
# | 3.12.2010 16:50|2010-12-03|2010-12-03 16:50:00|
# | 7.12.2010 12:28|2010-12-07|2010-12-07 12:28:00|
# | 8.12.2010 15:02|2010-12-08|2010-12-08 15:02:00|
# |10.12.2010 09:53|2010-12-10|2010-12-10 09:53:00|
# |12.12.2010 13:32|2010-12-12|2010-12-12 13:32:00|
# +----------------+----------+-------------------+
# only showing top 5 rows


df_.printSchema()
# root
#  |-- InvoiceDate: string (nullable = true)
#  |-- Date: date (nullable = true)
#  |-- TimeStamp: timestamp (nullable = true)


# Extracting Year, Month, Day of Month, Day of Week:

df_ = df_\
    .withColumn("Year", F.year(F.col("TimeStamp"))) \
    .withColumn("Month", F.month(F.col("TimeStamp"))) \
    .withColumn("DayofMonth", F.dayofmonth(F.col("TimeStamp"))) \
    .withColumn("DayOfWeek", F.dayofweek(F.col("TimeStamp")))

df_.show(50)

# +----------------+----------+-------------------+----+-----+---+----------+---------+
# |     InvoiceDate|      Date|          TimeStamp|Year|Month|Day|DayofMonth|DayOfWeek|
# +----------------+----------+-------------------+----+-----+---+----------+---------+
# | 3.12.2010 16:50|2010-12-03|2010-12-03 16:50:00|2010|   12|  3|         3|        6|
# | 7.12.2010 12:28|2010-12-07|2010-12-07 12:28:00|2010|   12|  7|         7|        3|
# | 8.12.2010 15:02|2010-12-08|2010-12-08 15:02:00|2010|   12|  8|         8|        4|
# |10.12.2010 09:53|2010-12-10|2010-12-10 09:53:00|2010|   12| 10|        10|        6|
# |12.12.2010 13:32|2010-12-12|2010-12-12 13:32:00|2010|   12| 12|        12|        1|
# +----------------+----------+-------------------+----+-----+---+----------+---------+


# Extract Weekend or Weekday information:

df_ = df_.withColumn("WeekdayType",
                     F.when(F.col("DayOfWeek").isin(1, 7), "Weekend") # Sunday veya Saturday
                      .otherwise("Weekday")) # Other Days

df_.show(10)

# +----------------+----------+-------------------+----+-----+---+----------+---------+-----------+
# |     InvoiceDate|      Date|          TimeStamp|Year|Month|Day|DayofMonth|DayOfWeek|WeekdayType|
# +----------------+----------+-------------------+----+-----+---+----------+---------+-----------+
# | 3.12.2010 16:50|2010-12-03|2010-12-03 16:50:00|2010|   12|  3|         3|        6|    Weekday|
# | 7.12.2010 12:28|2010-12-07|2010-12-07 12:28:00|2010|   12|  7|         7|        3|    Weekday|
# | 8.12.2010 15:02|2010-12-08|2010-12-08 15:02:00|2010|   12|  8|         8|        4|    Weekday|
# |10.12.2010 09:53|2010-12-10|2010-12-10 09:53:00|2010|   12| 10|        10|        6|    Weekday|
# |12.12.2010 13:32|2010-12-12|2010-12-12 13:32:00|2010|   12| 12|        12|        1|    Weekend|
# |15.12.2010 13:21|2010-12-15|2010-12-15 13:21:00|2010|   12| 15|        15|        4|    Weekday|
# |16.12.2010 08:41|2010-12-16|2010-12-16 08:41:00|2010|   12| 16|        16|        5|    Weekday|
# |17.12.2010 09:52|2010-12-17|2010-12-17 09:52:00|2010|   12| 17|        17|        6|    Weekday|
# | 9.01.2011 11:43|2011-01-09|2011-01-09 11:43:00|2011|    1|  9|         9|        1|    Weekend|
# |11.01.2011 11:38|2011-01-11|2011-01-11 11:38:00|2011|    1| 11|        11|        3|    Weekday|
# +----------------+----------+-------------------+----+-----+---+----------+---------+-----------+
# only showing top 10 rows

# Day of Week Name
df_ = df_.withColumn("DayOfWeekName", F.date_format(F.col("TimeStamp"), "EEEE"))

# Month Name
df_ = df_.withColumn("MonthName", F.date_format(F.col("TimeStamp"), "MMMM"))

# Quarter
df_ = df_.withColumn("Quarter", F.quarter(F.col("TimeStamp")))

# Week of Year
df_ = df_.withColumn("WeekOfYear", F.weekofyear(F.col("TimeStamp")))

# Day of Year
df_ = df_.withColumn("DayOfYear", F.dayofyear(F.col("TimeStamp")))

# Hour, Minute, and Second
df_ = df_.withColumn("Hour", F.hour(F.col("TimeStamp"))) \
         .withColumn("Minute", F.minute(F.col("TimeStamp"))) \
         .withColumn("Second", F.second(F.col("TimeStamp")))

df_.show(10)

# +----------------+----------+-------------------+----+-----+---+----------+---------+-----------+-------------+---------+-------+----------+---------+----+------+------+
# |     InvoiceDate|      Date|          TimeStamp|Year|Month|Day|DayofMonth|DayOfWeek|WeekdayType|DayOfWeekName|MonthName|Quarter|WeekOfYear|DayOfYear|Hour|Minute|Second|
# +----------------+----------+-------------------+----+-----+---+----------+---------+-----------+-------------+---------+-------+----------+---------+----+------+------+
# | 3.12.2010 16:50|2010-12-03|2010-12-03 16:50:00|2010|   12|  3|         3|        6|    Weekday|       Friday| December|      4|        48|      337|  16|    50|     0|
# | 7.12.2010 12:28|2010-12-07|2010-12-07 12:28:00|2010|   12|  7|         7|        3|    Weekday|      Tuesday| December|      4|        49|      341|  12|    28|     0|
# | 8.12.2010 15:02|2010-12-08|2010-12-08 15:02:00|2010|   12|  8|         8|        4|    Weekday|    Wednesday| December|      4|        49|      342|  15|     2|     0|
# |10.12.2010 09:53|2010-12-10|2010-12-10 09:53:00|2010|   12| 10|        10|        6|    Weekday|       Friday| December|      4|        49|      344|   9|    53|     0|
# |12.12.2010 13:32|2010-12-12|2010-12-12 13:32:00|2010|   12| 12|        12|        1|    Weekend|       Sunday| December|      4|        49|      346|  13|    32|     0|
# |15.12.2010 13:21|2010-12-15|2010-12-15 13:21:00|2010|   12| 15|        15|        4|    Weekday|    Wednesday| December|      4|        50|      349|  13|    21|     0|
# |16.12.2010 08:41|2010-12-16|2010-12-16 08:41:00|2010|   12| 16|        16|        5|    Weekday|     Thursday| December|      4|        50|      350|   8|    41|     0|
# |17.12.2010 09:52|2010-12-17|2010-12-17 09:52:00|2010|   12| 17|        17|        6|    Weekday|       Friday| December|      4|        50|      351|   9|    52|     0|
# | 9.01.2011 11:43|2011-01-09|2011-01-09 11:43:00|2011|    1|  9|         9|        1|    Weekend|       Sunday|  January|      1|         1|        9|  11|    43|     0|
# |11.01.2011 11:38|2011-01-11|2011-01-11 11:38:00|2011|    1| 11|        11|        3|    Weekday|      Tuesday|  January|      1|         2|       11|  11|    38|     0|
# +----------------+----------+-------------------+----+-----+---+----------+---------+-----------+-------------+---------+-------+----------+---------+----+------+------+
# only showing top 10 rows





