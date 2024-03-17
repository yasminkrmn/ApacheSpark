import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

pyspark = SparkSession.builder \
    .appName("DataUnderstanding") \
    .master("local[4]") \
    .config('spark.executor.memory', '4g') \
    .config('spark.driver.memory', '4g') \
    .getOrCreate()

sc = pyspark.sparkContext

#---------------------------------
# 1. Importing Data
#---------------------------------

df_train = pyspark.read \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .option('sep', ',') \
    .csv("8_DataPreprocessing/adult.data")

df_train.show(3)
# +---+-----------------+--------+----------+-------------+-------------------+------------------+--------------+------+-----+------------+------------+--------------+--------------+------+
# |age|        workclass|  fnlwgt| education|education_num|     marital_status|        occupation|  relationship|  race|  sex|capital_gain|capital_loss|hours_per_week|native_country|output|
# +---+-----------------+--------+----------+-------------+-------------------+------------------+--------------+------+-----+------------+------------+--------------+--------------+------+
# | 39|        State-gov| 77516.0| Bachelors|         13.0|      Never-married|      Adm-clerical| Not-in-family| White| Male|      2174.0|         0.0|          40.0| United-States| <=50K|
# | 50| Self-emp-not-inc| 83311.0| Bachelors|         13.0| Married-civ-spouse|   Exec-managerial|       Husband| White| Male|         0.0|         0.0|          13.0| United-States| <=50K|
# | 38|          Private|215646.0|   HS-grad|          9.0|           Divorced| Handlers-cleaners| Not-in-family| White| Male|         0.0|         0.0|          40.0| United-States| <=50K|
# +---+-----------------+--------+----------+-------------+-------------------+------------------+--------------+------+-----+------------+------------+--------------+--------------+------+
# only showing top 3 rows

df_test = pyspark.read \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .option('sep', ',') \
    .csv("8_DataPreprocessing/adult.test")

df_test.show(3)
# +---+----------+--------+-----------+-------------+-------------------+------------------+------------+------+-----+------------+------------+--------------+--------------+-------+
# |age| workclass|  fnlwgt|  education|education-num|     marital-status|        occupation|relationship|  race|  sex|capital-gain|capital-loss|hours-per-week|native-country| output|
# +---+----------+--------+-----------+-------------+-------------------+------------------+------------+------+-----+------------+------------+--------------+--------------+-------+
# | 25|   Private|226802.0|       11th|          7.0|      Never-married| Machine-op-inspct|   Own-child| Black| Male|         0.0|         0.0|          40.0| United-States| <=50K.|
# | 38|   Private| 89814.0|    HS-grad|          9.0| Married-civ-spouse|   Farming-fishing|     Husband| White| Male|         0.0|         0.0|          50.0| United-States| <=50K.|
# | 28| Local-gov|336951.0| Assoc-acdm|         12.0| Married-civ-spouse|   Protective-serv|     Husband| White| Male|         0.0|         0.0|          40.0| United-States|  >50K.|
# +---+----------+--------+-----------+-------------+-------------------+------------------+------------+------+-----+------------+------------+--------------+--------------+-------+

#---------------------------------
# 2. Merging Data
#---------------------------------

df = df_train.union(df_test)
df.limit(5).toPandas().head()
#    age          workclass    fnlwgt  ... hours_per_week  native_country  output
# 0   39          State-gov   77516.0  ...           40.0   United-States   <=50K
# 1   50   Self-emp-not-inc   83311.0  ...           13.0   United-States   <=50K
# 2   38            Private  215646.0  ...           40.0   United-States   <=50K
# 3   53            Private  234721.0  ...           40.0   United-States   <=50K
# 4   28            Private  338409.0  ...           40.0            Cuba   <=50K
# [5 rows x 15 columns]

df_train.count()+df_test.count() == df.count()
# True

#---------------------------------
# 3. Checking Schema
#---------------------------------

df.printSchema()
#  |-- age: integer (nullable = true)
#  |-- workclass: string (nullable = true)
#  |-- fnlwgt: double (nullable = true)
#  |-- education: string (nullable = true)
#  |-- education_num: double (nullable = true)
#  |-- marital_status: string (nullable = true)
#  |-- occupation: string (nullable = true)
#  |-- relationship: string (nullable = true)
#  |-- race: string (nullable = true)
#  |-- sex: string (nullable = true)
#  |-- capital_gain: double (nullable = true)
#  |-- capital_loss: double (nullable = true)
#  |-- hours_per_week: double (nullable = true)
#  |-- native_country: string (nullable = true)
#  |-- output: string (nullable = true)

df_train.toPandas().dtypes

#---------------------------------
# 4. Analyzing Numerical Columns
#---------------------------------

def find_numerical_columns(dataframe):
    """
    Takes a Spark DataFrame and returns the names of columns with numeric data types.

    Parameters:
        - df: Spark DataFrame.

    Return:
        - numerical_columns: List of column names with numeric data types.

    """
    numerical_types = ('integer', 'double', 'float')
    numerical_columns = [column_name for column_name, column_type in dataframe.dtypes if column_type in numerical_types]

    return numerical_columns


numerical_columns = find_numerical_columns(df)

#---------------------------------
# 4.1 Statistical Summary
#---------------------------------

df[numerical_columns].summary().show()

# +-------+------------------+------------------+------------------+-----------------+------------------+
# |summary|            fnlwgt|     education_num|      capital_gain|     capital_loss|    hours_per_week|
# +-------+------------------+------------------+------------------+-----------------+------------------+
# |  count|             48842|             48842|             48842|            48842|             48842|
# |   mean|189664.13459727284|10.078088530363212|1079.0676262233324|87.50231358257237|40.422382375824085|
# | stddev|105604.02542315733| 2.570972755592263| 7452.019057655401|403.0045521243599|  12.3914440242523|
# |    min|           12285.0|               1.0|               0.0|              0.0|               1.0|
# |    25%|          117529.0|               9.0|               0.0|              0.0|              40.0|
# |    50%|          178137.0|              10.0|               0.0|              0.0|              40.0|
# |    75%|          237608.0|              12.0|               0.0|              0.0|              45.0|
# |    max|         1490400.0|              16.0|           99999.0|           4356.0|              99.0|
# +-------+------------------+------------------+------------------+-----------------+------------------+


#---------------------------------
# 4.2 Null Variable Detection:
#---------------------------------

null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show()

# +---+---------+------+---------+-------------+--------------+----------+------------+----+---+------------+------------+--------------+--------------+------+
# |age|workclass|fnlwgt|education|education_num|marital_status|occupation|relationship|race|sex|capital_gain|capital_loss|hours_per_week|native_country|output|
# +---+---------+------+---------+-------------+--------------+----------+------------+----+---+------------+------------+--------------+--------------+------+
# |  0|        0|     0|        0|            0|             0|         0|           0|   0|  0|           0|           0|             0|             0|     0|
# +---+---------+------+---------+-------------+--------------+----------+------------+----+---+------------+------------+--------------+--------------+------+


#---------------------------------
# 4.3 Outlier Detection:
#---------------------------------

from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, DoubleType, FloatType, LongType

def detect_outliers_and_create_new_df(df):
    """
    Detects outliers in all numerical columns of a DataFrame using the IQR method and returns a new DataFrame
    with additional columns indicating the presence of outliers for each numerical column.

    Parameters:
    df (DataFrame): The input DataFrame.

    Returns:
    DataFrame: A new DataFrame with additional columns indicating the presence of outliers for each numerical column.
    """
    # Create a copy of the DataFrame to avoid modifying the original
    new_df = df

    for column_name in df.columns:
        # Check if the column is of a numeric type
        if isinstance(df.schema[column_name].dataType, (IntegerType, DoubleType, FloatType, LongType)):
            # Calculate the first and third quartiles
            quantiles = df.approxQuantile(column_name, [0.25, 0.75], 0.05)
            Q1, Q3 = quantiles
            IQR = Q3 - Q1

            # Calculate lower and upper bounds for outliers
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            # Flag rows as outliers or not and add to the new DataFrame
            new_df = new_df.withColumn(f"{column_name}_outlier",
                                       when((col(column_name) < lower_bound) | (col(column_name) > upper_bound), True)
                                       .otherwise(False))
    return new_df

outlier_df = detect_outliers_and_create_new_df(df[numerical_columns])
outlier_df.show(5)
# +--------+-------------+------------+------------+--------------+--------------+---------------------+--------------------+--------------------+----------------------+
# |  fnlwgt|education_num|capital_gain|capital_loss|hours_per_week|fnlwgt_outlier|education_num_outlier|capital_gain_outlier|capital_loss_outlier|hours_per_week_outlier|
# +--------+-------------+------------+------------+--------------+--------------+---------------------+--------------------+--------------------+----------------------+
# | 77516.0|         13.0|      2174.0|         0.0|          40.0|         false|                false|                true|               false|                 false|
# | 83311.0|         13.0|         0.0|         0.0|          13.0|         false|                false|               false|               false|                  true|
# |215646.0|          9.0|         0.0|         0.0|          40.0|         false|                false|               false|               false|                 false|
# |234721.0|          7.0|         0.0|         0.0|          40.0|         false|                false|               false|               false|                 false|
# |338409.0|         13.0|         0.0|         0.0|          40.0|         false|                false|               false|               false|                 false|
# +--------+-------------+------------+------------+--------------+--------------+---------------------+--------------------+--------------------+----------------------+
# only showing top 5 rows


#---------------------------------
# 5. Analyzing Categorical Columns
#---------------------------------

def find_string_columns(dataframe):
    """
    Takes a Spark DataFrame and returns the names of columns with string data types.

    Parameters:
        - df: Spark DataFrame.

    Return:
        - string_columns: List of column names with string data types.
    """
    string_type = 'string'
    string_columns = [column_name for column_name, column_type in dataframe.dtypes if column_type == string_type]

    return string_columns

string_columns = find_string_columns(df)
# ['workclass', 'education', 'marital_status', 'occupation', 'relationship', 'race', 'sex', 'native_country', 'output']


#---------------------------------
# 5.1 Analyzing Frequency
#---------------------------------
def count_frequency(dataframe):
    """
    Counts the frequency of values in all string type columns in the DataFrame and displays the results.

    Parameters:
    - dataframe: The Spark DataFrame whose string columns' frequencies are to be counted.
    """
    string_columns = find_string_columns(df)
    for col in string_columns:
        dataframe.groupBy(F.col(col)).count().show()

count_frequency(df)
# Example Output:
# +-----------------+-----+
# |        workclass|count|
# +-----------------+-----+
# |        State-gov| 1981|
# |      Federal-gov| 1432|
# | Self-emp-not-inc| 3862|
# |        Local-gov| 3136|
# |          Private|33906|
# |                ?| 2799|   Why???
# |     Self-emp-inc| 1695|
# |      Without-pay|   21| # Poor class
# |     Never-worked|   10| # Poor class
# +-----------------+-----+

# ....


# +-------+-----+
# | output|count|
# +-------+-----+
# |   >50K| 7841|
# |  <=50K|24720|
# |  >50K.| 3846|
# | <=50K.|12435| #Remove '.'
# +-------+-----+


#---------------------------------
# 5.1 Analyzing Frequency with Histogram
#---------------------------------

import matplotlib.pyplot as plt

def plot_string_column_histograms(dataframe):
    """
    Plots histograms for the frequency of values in all string type columns in the DataFrame.

    Parameters:
    - dataframe: The Spark DataFrame whose string columns' frequencies are to be plotted.
    """
    string_columns = find_string_columns(dataframe)

    for col_name in string_columns:
        # Count Frequency
        frequencies = df.groupBy(col_name).count().collect()

        # Export values and frequencies into separate lists
        values = [row[col_name] for row in frequencies]
        counts = [row['count'] for row in frequencies]

        # Plot Histogram
        plt.figure(figsize=(10, 6))
        plt.bar(values, counts, color='skyblue')
        plt.xlabel('Values')
        plt.ylabel('Frequency')
        plt.xticks(rotation=45, ha="right")
        plt.title(f'Histogram of {col_name}')
        plt.show()

plot_string_column_histograms(df)





