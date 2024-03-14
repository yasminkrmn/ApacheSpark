import findspark
findspark.init()
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[4]").setAppName("BroadcastVariables")
sc = SparkContext(conf=conf).getOrCreate()


def read_products(filename="5_Transformations/products.csv"):
    with open(filename, "r") as file:
        product_id_name = {int(line.split(",")[0]): line.split(",")[2]
                           for line in file
                           if "productId" not in line}
    return product_id_name

# Call the function and store the result
product_id_name = read_products()

# Broadcast the dictionary across a Spark cluster
products_broadcast = sc.broadcast(product_id_name)

# Retrieve the product name by ID using the broadcast variable
product_name_by_id = products_broadcast.value.get(1)

# Import order_items data
order_items_rdd = sc.textFile("5_Transformations/order_items.csv") \
    .filter(lambda line: "orderItemName" not in line)

order_items_rdd.take(5)
# Out[14]:
# ['1,1,957,1,299.98,299.98',
#  '2,2,1073,1,199.99,199.99',
#  '3,2,502,5,250.0,50.0',
#  '4,2,403,1,129.99,129.99',
#  '5,4,897,2,49.98,24.99']

# Convert products data to pair RDD
def convert_to_pairRDD(line):
    """
    Converts the row to (product_id, product_price) pairs.
    """
    product_id = int(line.split(",")[2])
    product_price = float(line.split(",")[-2])
    return product_id, product_price

orders_pairRDD = order_items_rdd.map(convert_to_pairRDD)
orders_pairRDD.take(5)
# Out[17]: [(957, 299.98), (1073, 199.99), (502, 250.0), (403, 129.99), (897, 49.98)]

# -----------------------------------------------------------------------
# Non-Sequential Way:

# Top 10 Products with the Highest Revenue
sortedProducts = orders_pairRDD \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: -x[1])

sortedProducts.take(10)
# [(1004, 6929653.499999708),
#  (365, 4421143.019999639),
#  (957, 4118425.419999785),
#  (191, 3667633.1999997487),
#  (502, 3147800.0),
#  (1073, 3099844.999999871),
#  (403, 2891757.5399998166),
#  (1014, 2888993.9399996493),
#  (627, 1269082.649999932),
#  (565, 67830.0)]

top10Products_name = sortedProducts \
    .map(lambda line: (products_broadcast.value.get(line[0]), line[1])).take(10)

top10Products_name

# [('Field & Stream Sportsman 16 Gun Fire Safe', 6929653.499999708),
#  ('Perfect Fitness Perfect Rip Deck', 4421143.019999639),
#  ("Diamondback Women's Serene Classic Comfort Bi", 4118425.419999785),
#  ("Nike Men's Free 5.0+ Running Shoe", 3667633.1999997487),
#  ("Nike Men's Dri-FIT Victory Golf Polo", 3147800.0),
#  ('Pelican Sunstream 100 Kayak', 3099844.999999871),
#  ("Nike Men's CJ Elite 2 TD Football Cleat", 2891757.5399998166),
#  ("O'Brien Men's Neoprene Life Vest", 2888993.9399996493),
#  ("Under Armour Girls' Toddler Spine Surge Runni", 1269082.649999932),
#  ('adidas Youth Germany Black/Red Away Match Soc', 67830.0)]

# -----------------------------------------------------------------------
# Sequential Way:
top10_products = orders_pairRDD \
                    .reduceByKey(lambda x, y: x + y) \
                    .map(lambda x: (x[1], x[0])) \
                    .sortByKey(False) \
                    .map(lambda x: (products_broadcast.value.get(x[1]), x[0])) \
                    .take(10)

# Print Results
for product, revenue in top10_products:
    print(f"{product}: {revenue}")

# Field & Stream Sportsman 16 Gun Fire Safe: 6929653.499999708
# Perfect Fitness Perfect Rip Deck: 4421143.019999639
# Diamondback Women's Serene Classic Comfort Bi: 4118425.419999785
# Nike Men's Free 5.0+ Running Shoe: 3667633.1999997487
# Nike Men's Dri-FIT Victory Golf Polo: 3147800.0
# Pelican Sunstream 100 Kayak: 3099844.999999871
# Nike Men's CJ Elite 2 TD Football Cleat: 2891757.5399998166
# O'Brien Men's Neoprene Life Vest: 2888993.9399996493
# Under Armour Girls' Toddler Spine Surge Runni: 1269082.649999932
# adidas Youth Germany Black/Red Away Match Soc: 67830.0












