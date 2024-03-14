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

data = sc.textFile("4_Map_FlatMap/data.csv")
data.take(5)

data2 = data.filter(lambda x: "no" not in x)
data2.take(5)


# Calculating Average Salary by Occupation with mapValues and reduceByKey
def avgsalarybyoccupation(x):
    occupation = x.split(",")[3]
    salary = float(x.split(",")[-1])
    return occupation, salary

data3 = data2.map(lambda x : avgsalarybyoccupation(x))
data3.take(5)
# [('Professor', 3500.0),
#  ('Officer', 4200.0),
#  ('Manager', 9000.0),
#  ('Technician', 4200.0),
#  ('Technician', 4800.0)]

data4 = data3.mapValues(lambda x: (x, 1))
data4.take(3)
# [('Professor', (3500.0, 1)),
#  ('Officer', (4200.0, 1)),
#  ('Manager', (9000.0, 1))]

data5 = data4.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
data5.take(5)
# [('Professor', (3500.0, 1)),
#  ('Officer', (12200.0, 3)),
#  ('Manager', (9000.0, 1)),
#  ('Technician', (16300.0, 3)),
#  ('Data Engineer', (12000.0, 1))]

data6 = data5.mapValues(lambda x: x[0] /x[1])
data6.take(5)
# [('Professor', 3500.0),
#  ('Officer', 4066.6666666666665),
#  ('Manager', 9000.0),
#  ('Technician', 5433.333333333333),
#  ('Data Engineer', 12000.0)]