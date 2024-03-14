import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

spark = SparkSession.builder \
    .appName("Transformations") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

data = sc.textFile("5_Transformations/OnlineRetail.csv")
data.take(5)

data2 = data.filter(lambda x: 'InvoiceNo' not in x)
data2.take(5)


# Calculating the Total Canceled Sales Amount

def canceled_amount(x):
    canceled_invoice = True if  (x.split(";")[0].startswith("C")) else False
    quantity = float(x.split(";")[3].strip(";"))
    price = float(x.split(";")[-3].strip(";").replace(",", "."))
    total = quantity * price
    return (canceled_invoice, total)

canceled_data = data2.map(lambda x : canceled_amount(x))
canceled_data.take(5)
canceled_total = canceled_data.reduceByKey(lambda x, y: x+y)
canceled_total.take(5)
# [(False, 10644560.424000263), (True, -896812.4900000116)]

canceled_total = canceled_total.filter(lambda x : x[0] == True)
canceled_total.take(5)
# [(True, -896812.4900000116)]

# Calculation in sequential order
canceled_total = canceled_data.reduceByKey(lambda x, y: x+y).filter(lambda x : x[0] == True)
# [(True, -896812.4900000116)]

sc.stop()