import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("DataCleaning") \
    .master("local[4]") \
    .config('spark.executor.memory', '4g') \
    .config('spark.driver.memory', '4g') \
    .getOrCreate()

sc = spark.sparkContext
#----------------------------
# 1. Importing Data:
#----------------------------

df = spark.read \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .option('sep', ',') \
    .csv('4_Map_FlatMap/data.csv')

df.show(5)
# +---+----------+---+----------+-------+------+
# | no|      name|age|       job|   city|salary|
# +---+----------+---+----------+-------+------+
# |  1|      Tony| 35| Professor|Newyork|  3500|
# |  2|Christelle| 42|   Officer| Madrid|  4200|
# |  3|       Tom| 30|   Manager|  Paris|  9000|
# |  4|     Cindy| 29|Technician|Newyork|  4200|
# |  5|      Lulu| 23|Technician| Berlin|  4800|
# +---+----------+---+----------+-------+------+

#----------------------------
# 2. Creating New Column:
#----------------------------
df = df\
    .withColumn("financial_status", when(col("salary" ) > 7000, 'whealthy') \
    .otherwise('not_whealthy'))
df.show(5)

# +---+----------+---+----------+-------+------+----------------+
# | no|      name|age|       job|   city|salary|financial_status|
# +---+----------+---+----------+-------+------+----------------+
# |  1|      Tony| 35| Professor|Newyork|  3500|    not_whealthy|
# |  2|Christelle| 42|   Officer| Madrid|  4200|    not_whealthy|
# |  3|       Tom| 30|   Manager|  Paris|  9000|        whealthy|
# |  4|     Cindy| 29|Technician|Newyork|  4200|    not_whealthy|
# |  5|      Lulu| 23|Technician| Berlin|  4800|    not_whealthy|
# +---+----------+---+----------+-------+------+----------------+
# only showing top 5 rows

#----------------------------
# 3. StringIndexer:
#----------------------------
from pyspark.ml.feature import StringIndexer

job_indexer = StringIndexer() \
    .setInputCol('job') \
    .setOutputCol('job_indexer')

model = job_indexer.fit(df)
df = model.transform(df)
df.show(5)
# +---+----------+---+----------+-------+------+----------------+-----------+
# | no|      name|age|       job|   city|salary|financial_status|job_indexer|
# +---+----------+---+----------+-------+------+----------------+-----------+
# |  1|      Tony| 35| Professor|Newyork|  3500|    not_whealthy|        8.0|
# |  2|Christelle| 42|   Officer| Madrid|  4200|    not_whealthy|        0.0|
# |  3|       Tom| 30|   Manager|  Paris|  9000|        whealthy|        6.0|
# |  4|     Cindy| 29|Technician|Newyork|  4200|    not_whealthy|        1.0|
# |  5|      Lulu| 23|Technician| Berlin|  4800|    not_whealthy|        1.0|
# +---+----------+---+----------+-------+------+----------------+-----------+

