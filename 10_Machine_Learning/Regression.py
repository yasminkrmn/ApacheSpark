import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Regression") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ",") \
    .csv("10_Machine_Learning/Advertising.csv")

df = df.drop("_c0")
df.show(5)

# +-----+-----+---------+-----+
# |   TV|Radio|Newspaper|Sales|
# +-----+-----+---------+-----+
# |230.1| 37.8|     69.2| 22.1|
# | 44.5| 39.3|     45.1| 10.4|
# | 17.2| 45.9|     69.3|  9.3|
# |151.5| 41.3|     58.5| 18.5|
# |180.8| 10.8|     58.4| 12.9|
# +-----+-----+---------+-----+
# only showing top 5 rows

#-------------------------------
# 1. Changing Column Name:
#-------------------------------

df = df.selectExpr("TV", "Radio", "Newspaper", "Sales as label")
df.show(5)
# +-----+-----+---------+-----+
# |   TV|Radio|Newspaper|label|
# +-----+-----+---------+-----+
# |230.1| 37.8|     69.2| 22.1|
# | 44.5| 39.3|     45.1| 10.4|
# | 17.2| 45.9|     69.3|  9.3|
# |151.5| 41.3|     58.5| 18.5|
# |180.8| 10.8|     58.4| 12.9|
# +-----+-----+---------+-----+
# only showing top 5 rows


features = ["TV", "Radio", "Newspaper"]
label = ["label"]

df.describe().show()
# +-------+-----------------+------------------+------------------+------------------+
# |summary|               TV|             Radio|         Newspaper|             label|
# +-------+-----------------+------------------+------------------+------------------+
# |  count|              200|               200|               200|               200|
# |   mean|         147.0425|23.264000000000024|30.553999999999995|14.022500000000003|
# | stddev|85.85423631490805|14.846809176168728| 21.77862083852283| 5.217456565710477|
# |    min|              0.7|               0.0|               0.3|               1.6|
# |    max|            296.4|              49.6|             114.0|              27.0|
# +-------+-----------------+------------------+------------------+------------------+


#-------------------------------
# 2. Data Preprocessing:
#-------------------------------

# Vector Assembler:
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler() \
     .setInputCols(features) \
     .setOutputCol("features")

#-------------------------------
# 2. Model Building:
#-------------------------------
from pyspark.ml.regression import LinearRegression

model = LinearRegression().setFeaturesCol('features').setLabelCol('label')

#-------------------------------
# 3. PipeLine:
#-------------------------------
from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[assembler, model])

#-------------------------------
# 4. Train-Test Split:
#-------------------------------

train_df, test_df = df.randomSplit([0.8, 0.2], seed=142)

#-------------------------------
# 5. Model Training:
#-------------------------------

pipeline_model = pipeline.fit(train_df)

#-------------------------------
# 6. Model Testing:
#-------------------------------
predictions = pipeline_model.transform(test_df)
predictions.show(5)
# +----+-----+---------+-----+----------------+------------------+
# |  TV|Radio|Newspaper|label|        features|        prediction|
# +----+-----+---------+-----+----------------+------------------+
# | 7.3| 28.1|     41.4|  5.5| [7.3,28.1,41.4]| 8.555798542923517|
# |11.7| 36.9|     45.2|  7.3|[11.7,36.9,45.2]| 10.37344884445182|
# |13.2| 15.9|     49.6|  5.6|[13.2,15.9,49.6]| 6.582036557018884|
# |17.9| 37.6|     21.6|  8.0|[17.9,37.6,21.6]|10.779185177913398|
# |27.5|  1.6|     20.7|  6.9| [27.5,1.6,20.7]| 4.596780697825989|
# +----+-----+---------+-----+----------------+------------------+
# only showing top 5 rows

#-------------------------------
# 6. Taking Model from PipeLine:
#-------------------------------

pipeline_model.stages
# [VectorAssembler_87459d7b7856,
#  LinearRegressionModel: uid=LinearRegression_50c36e89956f, numFeatures=3]

lr_model = pipeline_model.stages[1]

lr_model.coefficients
# DenseVector([0.0453, 0.1838, 0.0002])
lr_model.intercept
# 3.052755686737055
lr_model.summary.r2
# 0.8934149256561611
lr_model.summary.rootMeanSquaredError
# 1.6678086614009462
lr_model.summary.pValues
# [0.0, 0.0, 0.9796345762597278, 6.661338147750939e-16]
lr_model.summary.tValues
# [28.879534203027166,
#  18.75276727359823,
#  0.025567752447480316,
#  8.986644220874025]

#-------------------------------
# 6. Model Tuning:
#-------------------------------
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

param_grid = ParamGridBuilder() \
    .addGrid(model.aggregationDepth, [2, 4]) \
    .addGrid(model.elasticNetParam, [0, 0.02]) \
    .addGrid(model.epsilon, [1.35, 1.55]) \
    .addGrid(model.maxIter, [10, 20]) \
    .addGrid(model.regParam, [0.1, 0.01]) \
    .addGrid(model.solver, ['auto', 'normal', 'l-bfgs']) \
    .build()

cv = CrossValidator() \
    .setEstimator(pipeline) \
    .setEvaluator(RegressionEvaluator()) \
    .setEstimatorParamMaps(param_grid) \
    .setNumFolds(5) \
    .setParallelism(2)

train_df.cache()
test_df.cache()

cv_model = cv.fit(train_df)
cv_model.transform(test_df).show(5)
# +----+-----+---------+-----+----------------+------------------+
# |  TV|Radio|Newspaper|label|        features|        prediction|
# +----+-----+---------+-----+----------------+------------------+
# | 7.3| 28.1|     41.4|  5.5| [7.3,28.1,41.4]| 8.567865942938234|
# |11.7| 36.9|     45.2|  7.3|[11.7,36.9,45.2]|10.381808090636463|
# |13.2| 15.9|     49.6|  5.6|[13.2,15.9,49.6]| 6.600247676804145|
# |17.9| 37.6|     21.6|  8.0|[17.9,37.6,21.6]|10.782973712155666|
# |27.5|  1.6|     20.7|  6.9| [27.5,1.6,20.7]| 4.615491624552024|
# +----+-----+---------+-----+----------------+------------------+
# only showing top 5 rows

best_model = cv_model.bestModel
best_lr_model = best_model.stages[-1]
best_lr_model.intercept
# 3.0714666273469753
best_lr_model.summary.r2
# 0.893411272871215
best_lr_model.explainParams().split('\n')
# ['aggregationDepth: suggested depth for treeAggregate (>= 2). (default: 2, current: 2)',
#  'elasticNetParam: the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty. (default: 0.0, current: 0.02)',
#  'epsilon: The shape parameter to control the amount of robustness. Must be > 1.0. Only valid when loss is huber (default: 1.35, current: 1.35)',
#  'featuresCol: features column name. (default: features, current: features)',
#  'fitIntercept: whether to fit an intercept term. (default: True)',
#  'labelCol: label column name. (default: label, current: label)',
#  'loss: The loss function to be optimized. Supported options: squaredError, huber. (default: squaredError)',
#  'maxBlockSizeInMB: maximum memory in MB for stacking input data into blocks. Data is stacked within partitions. If more than remaining data size in a partition then it is adjusted to the data size. Default 0.0 represents choosing optimal value, depends on specific algorithm. Must be >= 0. (default: 0.0)',
#  'maxIter: max number of iterations (>= 0). (default: 100, current: 10)',
#  'predictionCol: prediction column name. (default: prediction)',
#  'regParam: regularization parameter (>= 0). (default: 0.0, current: 0.01)',
#  'solver: The solver algorithm for optimization. Supported options: auto, normal, l-bfgs. (default: auto, current: l-bfgs)',
#  'standardization: whether to standardize the training features before fitting the model. (default: True)',
#  'tol: the convergence tolerance for iterative algorithms (>= 0). (default: 1e-06)',
#  'weightCol: weight column name. If this is not set or empty, we treat all instance weights as 1.0. (undefined)']