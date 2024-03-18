import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("PipeLine") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ",") \
    .csv("4_Map_FlatMap/data.csv")
df.show()

df = df\
    .withColumn("financial_status", when(col("salary" ) > 7000, 'whealthy') \
    .otherwise('not_whealthy'))

#----------------------------
# 3. Building Pipeline:
#----------------------------

from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler

job_indexer = StringIndexer() \
    .setInputCol('job') \
    .setOutputCol('job_indexer') \
    .setHandleInvalid('skip')

city_indexer = StringIndexer() \
    .setInputCol('city') \
    .setOutputCol('city_indexer') \
    .setHandleInvalid('skip')

encoder = OneHotEncoder() \
    .setInputCols(["job_indexer", "city_indexer"]) \
    .setOutputCols(["job_ohe", "city_ohe"])

assembler = VectorAssembler() \
    .setInputCols(['age', 'salary', 'job_indexer', 'city_indexer']) \
    .setOutputCol("vectorized_features")

label_indexer = StringIndexer() \
    .setInputCol('financial_status') \
    .setOutputCol('label')

scaler = StandardScaler() \
    .setInputCol('vectorized_features') \
    .setOutputCol('features')

from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression() \
    .setFeaturesCol("features") \
    .setLabelCol("label") \
    .setPredictionCol("prediction")

train_df, test_df = df.randomSplit([0.8, 0.2], seed=142)
test_df.count()
from pyspark.ml import Pipeline

pipeline = Pipeline(stages = [job_indexer, city_indexer, encoder, assembler,
                            label_indexer, scaler, lr])

pipeline_model = pipeline.fit(train_df)

predictions = pipeline_model.transform(test_df)

predictions.select("prediction", "label").toPandas().head()

predictions.show()

#    prediction  label
# 0         0.0    0.0
# 1         1.0    1.0