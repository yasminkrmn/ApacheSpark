
################################# Downloading Data by Windows Powershell #################################

# Invoke-WebRequest -Uri "https://github.com/yasminkrmn/ApacheSpark/blob/main/2_Creating_RDD/OnlineRetail.csv" -OutFile "C:\Users\gayan\Documents\apache_spark\2_Creating_RDD\OnlineRetail.zip"

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

# Path of Zip File
zip_path = r"C:\Users\gayan\Documents\apache_spark\2_Creating_RDD\OnlineRetail.zip"
extract_folder = r"C:\Users\gayan\Documents\apache_spark\2_Creating_RDD"

# Unzip the zip file and extract its contents to the specified folder
with ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_folder)

# Reading one of the extracted files with Spark
file_to_read = os.path.join(extract_folder, os.listdir(extract_folder)[1])

# Read file as Spark DataFrame
df = sc.textFile(file_to_read) 

# Show RDD
df.take(5)
