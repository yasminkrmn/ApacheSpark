import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import warnings
warnings.filterwarnings("ignore")

spark = SparkSession.builder \
    .appName("Clustering") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option('sep', ',') \
    .format("csv") \
    .load('10_Machine_Learning/Mall_Customers.csv')

df.show(5)
# +----------+------+---+------------------+----------------------+
# |CustomerID|Gender|Age|Annual Income (k$)|Spending Score (1-100)|
# +----------+------+---+------------------+----------------------+
# |         1|  Male| 19|                15|                    39|
# |         2|  Male| 21|                15|                    81|
# |         3|Female| 20|                16|                     6|
# |         4|Female| 23|                16|                    77|
# |         5|Female| 31|                17|                    40|
# +----------+------+---+------------------+----------------------+
# only showing top 5 rows

assembler = VectorAssembler()\
    .setInputCols(["Annual Income (k$)", "Spending Score (1-100)"]) \
    .setOutputCol("feature")


scaler = StandardScaler() \
    .setInputCol("feature") \
    .setOutputCol("scaled_feature")

kmeans = KMeans() \
    .setSeed(142) \
    .setK(5) \
    .setFeaturesCol("scaled_feature") \
    .setPredictionCol("cluster")

pipeline = Pipeline() \
    .setStages([assembler, scaler, kmeans])

pipelineModel = pipeline.fit(df)

predictions = pipelineModel.transform(df)
predictions.show(5)

# +----------+------+---+------------------+----------------------+-----------+--------------------+-------+
# |CustomerID|Gender|Age|Annual Income (k$)|Spending Score (1-100)|    feature|      scaled_feature|cluster|
# +----------+------+---+------------------+----------------------+-----------+--------------------+-------+
# |         1|  Male| 19|                15|                    39|[15.0,39.0]|[0.57110829030364...|      4|
# |         2|  Male| 21|                15|                    81|[15.0,81.0]|[0.57110829030364...|      2|
# |         3|Female| 20|                16|                     6| [16.0,6.0]|[0.60918217632388...|      4|
# |         4|Female| 23|                16|                    77|[16.0,77.0]|[0.60918217632388...|      2|
# |         5|Female| 31|                17|                    40|[17.0,40.0]|[0.64725606234413...|      4|
# +----------+------+---+------------------+----------------------+-----------+--------------------+-------+
# only showing top 5 rows

predictions.groupBy("cluster").count().show(5)
# +-------+-----+
# |cluster|count|
# +-------+-----+
# |      1|   81|
# |      3|   39|
# |      4|   23|
# |      2|   22|
# |      0|   35|
# +-------+-----+


def findK(dataframe, k):
    kmeans = KMeans() \
        .setK(k) \
        .setSeed(142) \
        .setFeaturesCol("scaled_feature") \
        .setPredictionCol("cluster")

    pipeline = Pipeline() \
        .setStages([assembler, scaler, kmeans])

    pipelineModel = pipeline.fit(dataframe)

    return pipelineModel

for k in range(2,10):
    pipelineModel = findK(df, k)
    predictions = pipelineModel.transform(df)

    evaluator = ClusteringEvaluator() \
        .setFeaturesCol("feature") \
        .setPredictionCol("cluster") \
        .setMetricName("silhouette")

    score = evaluator.evaluate(predictions)
    print(k, score)

# 2 0.4481761873052607
# 3 0.6308238835691352
# 4 0.6566646049964463
# 5 0.7397829244623759 # OPTIMUM K VALUE
# 6 0.7259009760005913
# 7 0.7244663045086395
# 8 0.6878773263154949
# 9 0.624689178433656