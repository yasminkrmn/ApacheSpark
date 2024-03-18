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

city_indexer = StringIndexer() \
    .setInputCol('city') \
    .setOutputCol('city_indexer')

model = city_indexer.fit(df)
df = model.transform(df)
df.show(5)
# +---+----------+---+----------+-------+------+----------------+-----------+------------+
# | no|      name|age|       job|   city|salary|financial_status|job_indexer|city_indexer|
# +---+----------+---+----------+-------+------+----------------+-----------+------------+
# |  1|      Tony| 35| Professor|Newyork|  3500|    not_whealthy|        8.0|         0.0|
# |  2|Christelle| 42|   Officer| Madrid|  4200|    not_whealthy|        0.0|         4.0|
# |  3|       Tom| 30|   Manager|  Paris|  9000|        whealthy|        6.0|         1.0|
# |  4|     Cindy| 29|Technician|Newyork|  4200|    not_whealthy|        1.0|         0.0|
# |  5|      Lulu| 23|Technician| Berlin|  4800|    not_whealthy|        1.0|         3.0|
# +---+----------+---+----------+-------+------+----------------+-----------+------------+
# only showing top 5 rows


#-----------------------------
# 4. OneHot Encoder Estimator:
#-----------------------------
from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder() \
    .setInputCols(["job_indexer", "city_indexer"]) \
    .setOutputCols(["job_ohe", "city_ohe"])

model = encoder.fit(df)

df = model.transform(df)
df.show(5, truncate=False)

#-----------------------------
# 5. VectorAssembler:
#-----------------------------
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler() \
    .setInputCols(['age', 'salary', 'job_indexer', 'city_indexer']) \
    .setOutputCol("vectorized_features")

df = assembler.transform(df)
df.select('vectorized_features').show()
# +--------------------+
# | vectorized_features|
# +--------------------+
# |[35.0,3500.0,8.0,...|
# |[42.0,4200.0,0.0,...|
# |[30.0,9000.0,6.0,...|
# |[29.0,4200.0,1.0,...|
# |[23.0,4800.0,1.0,...|
# |[33.0,4250.0,0.0,...|
# |[29.0,7300.0,1.0,...|
# |[31.0,12000.0,3.0...|
# |[33.0,18000.0,2.0...|
# |[46.0,12000.0,5.0...|
# |[47.0,4800.0,9.0,...|
# |[43.0,4200.0,7.0,...|
# |[33.0,3750.0,0.0,...|
# |[37.0,14250.0,2.0...|
# |[41.0,8700.0,4.0,...|
# +--------------------+

#-----------------------------
# 6. Label Indexer:
#-----------------------------
label_indexer = StringIndexer() \
    .setInputCol('financial_status') \
    .setOutputCol('label_indexer')

model = label_indexer.fit(df)
df = model.transform(df)
df.select('vectorized_features', 'label_indexer').show(5)
# +--------------------+-------------+
# | vectorized_features|label_indexer|
# +--------------------+-------------+
# |[35.0,3500.0,8.0,...|          0.0|
# |[42.0,4200.0,0.0,...|          0.0|
# |[30.0,9000.0,6.0,...|          1.0|
# |[29.0,4200.0,1.0,...|          0.0|
# |[23.0,4800.0,1.0,...|          0.0|
# +--------------------+-------------+
# only showing top 5 rows

#-----------------------------
# 7. StandardScaler:
#-----------------------------
from pyspark.ml.feature import StandardScaler

scaler = StandardScaler() \
    .setInputCol('vectorized_features') \
    .setOutputCol('scaled_features')

model = scaler.fit(df)
df = model.transform(df)
df_ = df.select('scaled_features', 'label_indexer')

df_.show(5)
# +--------------------+-------------+
# |     scaled_features|label_indexer|
# +--------------------+-------------+
# |[5.00828097406012...|          0.0|
# |[6.00993716887214...|          0.0|
# |[4.29281226348010...|          1.0|
# |[4.14971852136410...|          0.0|
# |[3.29115606866808...|          0.0|
# +--------------------+-------------+
# only showing top 5 rows

#-----------------------------
# 8. Train-Test Split:
#-----------------------------
train_df, test_df = df_.randomSplit([0.8,0.2], seed=1)
df_.count()
# 15
train_df.count()
# 12
test_df.count()
# 3

#-----------------------------
# 9. Simple ML Model:
#-----------------------------

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression() \
    .setFeaturesCol("scaled_features") \
    .setLabelCol("label_indexer") \
    .setPredictionCol("prediction")

model = lr.fit(train_df)
prediction_df = model.transform(test_df)

prediction_df.show()
# +--------------------+-------------+--------------------+--------------------+----------+
# |     scaled_features|label_indexer|       rawPrediction|         probability|prediction|
# +--------------------+-------------+--------------------+--------------------+----------+
# |[4.43590600559610...|          1.0|[-67.374984527546...|[5.48802399277408...|       1.0|
# |[6.58231213733616...|          1.0|[-52.572924217271...|[1.47186885253376...|       1.0|
# |[6.72540587945216...|          0.0|[40.8410561243810...|           [1.0,0.0]|       0.0|
# +--------------------+-------------+--------------------+--------------------+----------+





