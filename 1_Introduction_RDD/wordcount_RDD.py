import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

spark = SparkSession.builder \
.master("local[4]") \
.appName("Wordcount_Rdd") \
.getOrCreate()

sc = spark.sparkContext

df_rdd = sc.textFile(r"C:\Users\gayan\Documents\apache_spark\omer_seyfettin_forsa_hikaye.txt")
df_rdd.take(5)
df_rdd.count()
# Out[12]: 103 --- counted line

words = df_rdd.flatMap(lambda line: line.split(" "))
words.take(2)
# Out[15]: ['Ömer', 'Seyfettin']


# map() transformation applies a function to each element of an RDD,
# transforming each element into a new form and resulting in a new RDD.
word_counts_RBK = words.map(lambda word: (word, 1))
word_counts_RBK.take(2)
# Out[17]: [('Ömer', 1), ('Seyfettin', 1)]

# reduceByKey() is a transformation that aggregates values for each key in a pair RDD using a specified reduce function.
word_counts_RBK_2 = word_counts_RBK.reduceByKey(lambda x, y: x + y)
word_counts_RBK_2.take(5)

# Out[22]: [('Ömer', 1), ('Seyfettin', 1), ('Forsa', 1), ('', 59), ('Akdeniz’in,', 1)]

word_counts_RBK_2 = word_counts_RBK_2.map(lambda x: (x[1], x[0]))
word_counts_RBK_2.take(5)
# Out[23]: [(1, 'Ömer'), (1, 'Seyfettin'), (1, 'Forsa'), (59, ''), (1, 'Akdeniz’in,')]

word_counts_RBK_2.sortByKey(False).take(5)
# Out[25]: [(59, ''), (33, 'bir'), (31, '–'), (8, 'yıl'), (6, 'diye')]


spark.stop()
