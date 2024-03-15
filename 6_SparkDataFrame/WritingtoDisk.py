import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ETL") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()


sc = spark.sparkContext

df =  spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep" , ",") \
    .csv("6_SparkDataFrame/simple_dirty_data.csv")

df.show(5)
# +---+-------+---+------+------------+------------+------+--------------------+
# | no|   name|age|gender|  occupation|        city|income|            property|
# +---+-------+---+------+------------+------------+------+--------------------+
# |  1| Pierre| 35|     M|       Staff|       Paris|  3500|                 car|
# |  2| carol | 42|     F|     Officer|      Annecy|  4200|           car|house|
# |  3|    Teo| 30|  NULL|    Engineer|        NULL|  9000|car|house|summerh...|
# |  4|Yasmine| 23|     F|Sales person|    Bordeaux|  4800|                 car|
# |  5|   Tony| 33|     M|     Officer|Montdidider |  3750|               house|
# +---+-------+---+------+------------+------------+------+--------------------+
# only showing top 5 rows


# Cleaning Data

from pyspark.sql import functions as F

df2 = df \
    .withColumn("name", F.trim(F.upper("name"))) \
    .withColumn("gender", F.when(F.col("gender").isNull(), 'U').otherwise(df.gender)) \
    .withColumn("city", F.when(F.col("city").isNull(), 'U').otherwise(F.trim(F.upper(df.city)))) \
    .withColumn("income", F.format_number(F.col("income"),2))

df2.show(5)

# +---+-------+---+------+------------+-----------+--------+--------------------+
# | no|   name|age|gender|  occupation|       city|  income|            property|
# +---+-------+---+------+------------+-----------+--------+--------------------+
# |  1| PIERRE| 35|     M|       Staff|      PARIS|3,500.00|                 car|
# |  2|  CAROL| 42|     F|     Officer|     ANNECY|4,200.00|           car|house|
# |  3|    TEO| 30|     U|    Engineer|          U|9,000.00|car|house|summerh...|
# |  4|YASMINE| 23|     F|Sales person|   BORDEAUX|4,800.00|                 car|
# |  5|   TONY| 33|     M|     Officer|MONTDIDIDER|3,750.00|               house|
# +---+-------+---+------+------------+-----------+--------+--------------------+
# only showing top 5 rows


# Writing to Disk

df2 \
    .coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "True") \
    .option("sep" , ",") \
    .csv("6_SparkDataFrame\simple_clean_data")