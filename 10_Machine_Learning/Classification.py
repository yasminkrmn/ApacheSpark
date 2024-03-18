import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Classification") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ",") \
    .csv("10_Machine_Learning/iris.csv")

df.show(5)
# +-------------+------------+-------------+------------+-----------+
# |SepalLengthCm|SepalWidthCm|PetalLengthCm|PetalWidthCm|    Species|
# +-------------+------------+-------------+------------+-----------+
# |          5.1|         3.5|          1.4|         0.2|Iris-setosa|
# |          4.9|         3.0|          1.4|         0.2|Iris-setosa|
# |          4.7|         3.2|          1.3|         0.2|Iris-setosa|
# |          4.6|         3.1|          1.5|         0.2|Iris-setosa|
# |          5.0|         3.6|          1.4|         0.2|Iris-setosa|
# +-------------+------------+-------------+------------+-----------+
# only showing top 5 rows

df.printSchema()
# root
#  |-- SepalLengthCm: double (nullable = true)
#  |-- SepalWidthCm: double (nullable = true)
#  |-- PetalLengthCm: double (nullable = true)
#  |-- PetalWidthCm: double (nullable = true)
#  |-- Species: string (nullable = true)

#-------------------------------
# 1. Data Preprocessing:
#-------------------------------

df.describe().show()
# +-------+------------------+-------------------+------------------+------------------+--------------+
# |summary|     SepalLengthCm|       SepalWidthCm|     PetalLengthCm|      PetalWidthCm|       Species|
# +-------+------------------+-------------------+------------------+------------------+--------------+
# |  count|               150|                150|               150|               150|           150|
# |   mean| 5.843333333333335| 3.0540000000000007|3.7586666666666693|1.1986666666666672|          NULL|
# | stddev|0.8280661279778637|0.43359431136217375| 1.764420419952262|0.7631607417008414|          NULL|
# |    min|               4.3|                2.0|               1.0|               0.1|   Iris-setosa|
# |    max|               7.9|                4.4|               6.9|               2.5|Iris-virginica|
# +-------+------------------+-------------------+------------------+------------------+--------------+

df.groupBy("Species").count().show()
# +---------------+-----+
# |        Species|count|
# +---------------+-----+
# | Iris-virginica|   50|
# |    Iris-setosa|   50|
# |Iris-versicolor|   50|
# +---------------+-----+


#-------------------------------
# 1.1 StringIndexer:
#-------------------------------
from pyspark.ml.feature import StringIndexer

indexer = StringIndexer() \
    .setInputCol("Species") \
    .setOutputCol("label") \
    .setHandleInvalid('skip')

df = indexer.fit(df).transform(df)
df.groupBy("label").count().show()
# +-----+-----+
# |label|count|
# +-----+-----+
# |  0.0|   50|
# |  1.0|   50|
# |  2.0|   50|
# +-----+-----+

#-------------------------------
# 1.2 VectorAssembler:
#-------------------------------

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler() \
    .setInputCols(['SepalLengthCm', 'SepalWidthCm', 'PetalLengthCm', 'PetalWidthCm']) \
    .setOutputCol('features')

df = assembler.transform(df)
df.show(5)
# +-------------+------------+-------------+------------+-----------+-----+-----------------+
# |SepalLengthCm|SepalWidthCm|PetalLengthCm|PetalWidthCm|    Species|label|         features|
# +-------------+------------+-------------+------------+-----------+-----+-----------------+
# |          5.1|         3.5|          1.4|         0.2|Iris-setosa|  0.0|[5.1,3.5,1.4,0.2]|
# |          4.9|         3.0|          1.4|         0.2|Iris-setosa|  0.0|[4.9,3.0,1.4,0.2]|
# |          4.7|         3.2|          1.3|         0.2|Iris-setosa|  0.0|[4.7,3.2,1.3,0.2]|
# |          4.6|         3.1|          1.5|         0.2|Iris-setosa|  0.0|[4.6,3.1,1.5,0.2]|
# |          5.0|         3.6|          1.4|         0.2|Iris-setosa|  0.0|[5.0,3.6,1.4,0.2]|
# +-------------+------------+-------------+------------+-----------+-----+-----------------+
# only showing top 5 rows

#-------------------------------
# 2. Train-Test Split:
#-------------------------------

traind_df, test_df = df.randomSplit([0.8, 0.2], seed=142)

#-------------------------------
# 3. Building Model:
#-------------------------------
from pyspark.ml.classification import LogisticRegression

classifier = LogisticRegression() \
    .setFeaturesCol("features") \
    .setLabelCol("label")

model = classifier.fit(traind_df)
predictions = model.transform(test_df)
predictions.select("label", "prediction").show(10)
# +-----+----------+
# |label|prediction|
# +-----+----------+
# |  0.0|       0.0|
# |  0.0|       0.0|
# |  0.0|       0.0|
# |  0.0|       0.0|
# |  0.0|       0.0|
# |  0.0|       0.0|
# |  0.0|       0.0|
# |  0.0|       0.0|
# |  1.0|       1.0|
# |  0.0|       0.0|
# +-----+----------+
# only showing top 10 rows

#-------------------------------
# 4. Model Evaluation:
#-------------------------------
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator() \
    .setLabelCol("label") \
    .setPredictionCol("prediction") \
    .setMetricName("accuracy")

accuracy = evaluator.evaluate(predictions)
print(accuracy)
# 0.88

