import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

pyspark = SparkSession.builder \
    .appName("DataCleaning") \
    .master("local[4]") \
    .config('spark.executor.memory', '4g') \
    .config('spark.driver.memory', '4g') \
    .getOrCreate()

sc = pyspark.sparkContext

#---------------------------------
# 1. Importing and Merging Data
#---------------------------------

df_train = pyspark.read \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .option('sep', ',') \
    .csv("8_DataPreprocessing/adult.data")


df_test = pyspark.read \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .option('sep', ',') \
    .csv("8_DataPreprocessing/adult.test")

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
# 2. Triming Spaces
#---------------------------------

# Trim the spaces from both ends for the specified string column.

for i in df.columns:
    df = df.withColumn(i, trim(col(i)))

#---------------------------------
# 2. Removing '.' in 'output' column
#---------------------------------

# # +-------+-----+
# # | output|count|
# # +-------+-----+
# # |   >50K| 7841|
# # |  <=50K|24720|
# # |  >50K.| 3846|
# # | <=50K.|12435| #Remove '.'
# # +-------+-----+

df = df \
    .withColumn('output', regexp_replace(col('output'), '>50K.', '>50K'))\
    .withColumn('output', regexp_replace(col('output'), '<=50K.', '<=50K'))

df.groupBy(col('output')).count().show()
# +------+-----+
# |output|count|
# +------+-----+
# | <=50K|37155|
# |  >50K|11687|
# +------+-----+


#---------------------------------------------
# 3. Checking '?' in Columns and Deleting Them
#---------------------------------------------

def report_columns_containing_value(df, value='?'):
    """
    Reports columns in a DataFrame that contain a specified value and counts the occurrences.

    Parameters:
    - df: The Spark DataFrame to be searched.
    - value: The value to search for in the DataFrame columns.

    """
    # Create a dictionary to hold the results
    results = {}

    for column in df.columns:
        # Count the number of rows containing a specific value
        count = df.filter(col(column).contains(value)).count()
        if count > 0:
            results[column] = count

    # Print Results
    if results:
        print(f"Columns containing '{value}':")
        for column, count in results.items():
            print(f"{column}: {count} occurrences")
    else:
        print(f"No columns contain '{value}'.")

report_columns_containing_value(df, '?')
# Columns containing '?':
# workclass: 2799 occurrences
# occupation: 2809 occurrences
# native_country: 857 occurrences

df_ = df \
    .filter(~(
    col('workclass').contains('?') |
    col('occupation').contains('?') |
    col('native_country').contains('?')

))
# Checking:
report_columns_containing_value(df_, '?')
# No columns contain '?'.


#---------------------------------
# 4. Removing Poor Class
#---------------------------------

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

df_ = df_.filter(~(
    col('workclass').contains('Without-pay') |
    col('workclass').contains('Never-worked') |
    col('occupation').contains('Armed-Forces') |
    col('native_country').contains('Holand-Netherlands')
))

# Checking:
df_.filter(col('workclass').contains('Without-pay')) == True
# False

#---------------------------------
# 2. Merging Class in 'education' Col
#---------------------------------

df_ = df_ \
    .withColumn('merged_education',
                when(col('education').isin('1st-4th','5th-6th', '7th-8th'), 'Elementary-School')
                .when(col('education').isin('9th', '10th', '11th', '12th'), 'High-school')
                .when(col('education').isin('Masters', 'Doctorate'), 'Postgraduate')
                .otherwise(col('education'))
                )

df_.select('education', 'merged_education').toPandas().head(6)
#        education   merged_education
# 0      Bachelors          Bachelors
# 1      Bachelors          Bachelors
# 2        HS-grad            HS-grad
# 3           11th        High-school
# 4      Bachelors          Bachelors
# 5        Masters       Postgraduate

#---------------------------------
# 5. Writing to Disk
#---------------------------------
df_ \
    .coalesce(1) \
    .write \
    .mode('overwrite') \
    .option('sep', ',') \
    .option('header', 'True') \
    .csv(r'8_DataPreprocessing\DataCleaning')





























