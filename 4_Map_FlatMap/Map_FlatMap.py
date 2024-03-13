import findspark
findspark.init()

from pyspark import SparkContext

sc = SparkContext(appName="Map_FlatMap", master="local[4]")

df = sc.textFile("4_Map_FlatMap/data.csv")

df.take(5)
# ['no,name,age,job,city,salary',
#  '1,Tony,35,Professor,Newyork,3500',
#  '2,Christelle,42,Officer,Madrid,4200',
#  '3,Tom,30,Manager,Paris,9000',
#  '4,Cindy,29,Technician,Newyork,4200']


# MAP
df.map(lambda x: x.upper()).take(5)
# ['NO,NAME,AGE,JOB,CITY,SALARY',
#  '1,TONY,35,PROFESSOR,NEWYORK,3500',
#  '2,CHRISTELLE,42,OFFICER,MADRID,4200',
#  '3,TOM,30,MANAGER,PARIS,9000',
#  '4,CINDY,29,TECHNICIAN,NEWYORK,4200']

# FLATMAP

df.flatMap(lambda x: x.split(",")).map(lambda x: x.upper()).take(10)
# ['NO', 'NAME', 'AGE', 'JOB', 'CITY', 'SALARY', '1', 'TONY', '35', 'PROFESSOR']