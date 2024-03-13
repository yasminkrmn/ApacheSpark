import findspark
from pyspark import SparkContext

findspark.init()

################################ Creating SparkContext ################################

# Method 1 - SparkSession
from pyspark.sql import SparkSession

pyspark = SparkSession.builder \
    .master("local[4]") \
    .appName("Creating_RDD") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()


sc = pyspark.sparkContext

rdd_1 = sc.parallelize([("Boncuk", 1), ("Aysur", 2), ("Zaferiko", 3), ("Jordan", 4)])
rdd_1.take(4)

sc.stop()

# Method 2 - SparkSession and SparkConf

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

conf = SparkConf() \
    .setMaster("local[4]") \
    .setAppName("Creating_RDD") \
    .setExecutorEnv("spark.executor.memory", "4g") \
    .set("spark.driver.memory", "4g")

pyspark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()

sc = pyspark.sparkContext

rdd_1 = sc.parallelize([("Boncuk", 1), ("Aysur", 2), ("Zaferiko", 3), ("Jordan", 4)])
rdd_1.take(4)

sc.stop()


# Method 3 - SparkContext and SparkConf
from pyspark.conf import SparkConf
from pyspark import SparkContext

sparkconf = SparkConf() \
    .setMaster("local[4]") \
    .setAppName("Creating_RDD") \
    .setExecutorEnv("spark.executor.memory", "4g") \
    .set("spark.driver.memory", "4g")

sc = SparkContext(conf=sparkconf)
sc = pyspark.sparkContext
sc.stop()

# RDD using Tuple
rdd_1 = sc.parallelize([("Boncuk", 1), ("Aysur", 2), ("Zaferiko", 3), ("Jordan", 4)])
rdd_1.take(4)
# Out[23]: [('Boncuk', 1), ('Aysur', 2), ('Zaferiko', 3), ('Jordan', 4)]

# RDD using List
rdd_2 = sc.parallelize([["Boncuk", 1], ["Aysur", 2], ["Zaferiko", 3], ["Jordan", 4]])
rdd_2.take(4)
# Out[25]: [['Boncuk', 1], ['Aysur', 2], ['Zaferiko', 3], ['Jordan', 4]]

rdd_2.count()
# Out[26]: 4

# RDD using DataFrame

my_dict = {"Students": ["Boncuk", "Aysur", "Zaferiko", "Jordan"],
           "Grades": [70, 80, 60, 10]}

import pandas as pd

df = pd.DataFrame(my_dict)
df

rdd_from_pandas = pyspark.createDataFrame(df)
rdd_from_pandas.show()
# |Students|Grades|
# +--------+------+
# |  Boncuk|    70|
# |   Aysur|    80|
# |Zaferiko|    60|
# |  Jordan|    10|
# +--------+------+

rdd_from_pandas = rdd_from_pandas.rdd
rdd_from_pandas.take(4)
# [Row(Students='Boncuk', Grades=70),
#  Row(Students='Aysur', Grades=80),
#  Row(Students='Zaferiko', Grades=60),
#  Row(Students='Jordan', Grades=10)]


################################# Downloading Data by Windows Powershell #################################

# Invoke-WebRequest -Uri "https://github.com/veribilimiokulu/udemy-apache-spark/raw/master/data/OnlineRetail.zip" -OutFile "C:\Users\gayan\Documents\apache_spark\2_Creating_RDD\OnlineRetail.zip"


################################# Importing ZIP File CSV Data #################################
from zipfile import ZipFile
import os

# Zipli dosyanın yolu
zip_path = r"C:\Users\gayan\Documents\apache_spark\2_Creating_RDD\OnlineRetail.zip"
extract_folder = r"C:\Users\gayan\Documents\apache_spark\2_Creating_RDD"

# Zipli dosyayı açma ve içeriğini belirtilen klasöre çıkarma
with ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_folder)

# Çıkarılan dosyalardan birini Spark ile okuma
# Bu örnek için, zip dosyasının içinde sadece bir dosya olduğunu varsayıyoruz
file_to_read = os.path.join(extract_folder, os.listdir(extract_folder)[1])

# Dosyayı Spark DataFrame olarak okuma
df = sc.textFile(file_to_read) # CSV formatı varsayılıyor, gerektiğinde değiştirin

# DataFrame'i göster
df.take(5)


