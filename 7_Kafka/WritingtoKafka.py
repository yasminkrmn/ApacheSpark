import findspark
findspark.init()

import os
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("WritetoKafka") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
    .getOrCreate()

sc = spark.sparkContext

df = spark.read.format("csv").option("header", "True").load("7_Kafka/Advertising.csv")
df.show(5)

# Kafka operates on a key-value pair model to allow message segmentation and efficient data processing across distributed systems.

df_2 = df.withColumn("key", col('ID')).drop('ID')\
    .select('key',
            concat(
                col('TV'), lit(','),
                      col('Radio'), lit(','),
                      col('Newspaper'), lit(','),
                      col('Sales')
                ).alias('value')
            )

df_2.show(5)

# +---+--------------------+
# |key|               value|
# +---+--------------------+
# |  1|230.1,37.8,69.2,22.1|
# |  2| 44.5,39.3,45.1,10.4|
# |  3|  17.2,45.9,69.3,9.3|
# |  4|151.5,41.3,58.5,18.5|
# |  5|180.8,10.8,58.4,12.9|
# +---+--------------------+
# only showing top 5 rows

# Writing to Kafka:
df_2 \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "myFirstTopic") \
    .save()











