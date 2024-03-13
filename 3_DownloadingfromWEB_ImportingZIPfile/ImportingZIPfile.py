
################################# Downloading Data by Windows Powershell #################################

# Invoke-WebRequest -Uri "https://github.com/veribilimiokulu/udemy-apache-spark/raw/master/data/OnlineRetail.zip" -OutFile "C:\Users\gayan\Documents\apache_spark\2_Creating_RDD\OnlineRetail.zip"


################################# Importing ZIP File CSV Data #################################
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

################################ Importing ZIP File ################################
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