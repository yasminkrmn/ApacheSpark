import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IntroductionDataframe") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

df = spark.read.text("1_Introduction_RDD/omer_seyfettin_forsa_hikaye.txt")
df.show(3, truncate=False)
# +----------------------+
# |                 value|
# +----------------------+
# |Ömer Seyfettin - Forsa|
# |                      |
# |Akdeniz’in, kahra.....|
# +----------------------+
# only showing top 3 rows


from pyspark.sql.functions import explode, split, col, desc

words = df.select(explode(split(col("value"), " ")).alias("word"))
words.show(5)
# +---------+
# |     word|
# +---------+
# |     Ömer|
# |Seyfettin|
# |        -|
# |    Forsa|
# |         |
# +---------+
# only showing top 5 rows

# ----------------------------------------------------------------------------------
# Using Sort:

words.groupBy("word").count().sort("count", ascending=False).show(5)
# +----+-----+
# |word|count|
# +----+-----+
# |    |   59|
# | bir|   33|
# |   –|   31|
# | yıl|    8|
# |diye|    6|
# +----+-----+
# only showing top 5 rows

# ----------------------------------------------------------------------------------
# Using OrderBy:
words.groupBy("word").count().orderBy(desc("count")).show(5)

# +----+-----+
# |word|count|
# +----+-----+
# |    |   59|
# | bir|   33|
# |   –|   31|
# | yıl|    8|
# |diye|    6|
# +----+-----+
# only showing top 5 rows

# ----------------------------------------------------------------------------------
# Using SQL Query:

# Save DataFrame as temp view
words.createOrReplaceTempView("words_table")

query = """
SELECT word, COUNT(word) AS count
FROM words_table
GROUP BY word
ORDER BY count DESC
LIMIT 5
"""

sc.stop()

spark.sql(query).show()




