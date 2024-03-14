# Joining Orders data and Products data

import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Transformations") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

# Importing Orders data by removing header
orders = sc.textFile("5_Transformations/order_items.csv")\
    .filter(lambda line: "orderItemName" not in line)

orders.take(5)

# Importing Products data by removing header
products = sc.textFile("5_Transformations/products.csv") \
    .filter(lambda line: "productId" not in line)

products.take(5)


# Converting Data to Pair RDD
def convertOrderstoPairRDD(line):
    orderItemName = line.split(",")[0],
    orderItemOrderId = line.split(",")[1],
    orderItemProductId = line.split(",")[2],
    orderItemQuantity = line.split(",")[3],
    orderItemSubTotal = line.split(",")[4],
    orderItemProductPrice = line.split(",")[5]
    return (orderItemProductId, (orderItemName,orderItemOrderId,orderItemQuantity,orderItemSubTotal,orderItemProductPrice))

pairRDD_orders = orders.map(convertOrderstoPairRDD)
pairRDD_orders.take(5)
# [(('957',), (('1',), ('1',), ('1',), ('299.98',), '299.98')),
#  (('1073',), (('2',), ('2',), ('1',), ('199.99',), '199.99')),
#  (('502',), (('3',), ('2',), ('5',), ('250.0',), '50.0')),
#  (('403',), (('4',), ('2',), ('1',), ('129.99',), '129.99')),
#  (('897',), (('5',), ('4',), ('2',), ('49.98',), '24.99'))]

def convertProductstoPairRDD(line):
    productId = line.split(",")[0],
    productCategoryId = line.split(",")[1],
    productName = line.split(",")[2],
    productDescription = line.split(",")[3],
    productPrice = line.split(",")[4],
    productImage = line.split(",")[5]
    return (productId, (productCategoryId,productName,productDescription,productPrice,productImage))

pairRDD_products = products.map(convertProductstoPairRDD)
pairRDD_products.take(5)

# Join
joinedRDD = pairRDD_orders.join(pairRDD_products)
joinedRDD.take(5)

pairRDD_orders.count()
# Out[16]: 172198
pairRDD_products.count()
# Out[17]: 1345
joinedRDD.count()
# Out[18]: 172198