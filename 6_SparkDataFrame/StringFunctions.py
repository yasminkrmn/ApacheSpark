import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("StringFunctions") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

df = spark.read \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .option("sept", ",") \
    .csv("6_SparkDataFrame/simple_dirty_data.csv")

df.show(5)

# +---+--------+----+-------+------------+-----------+-------+--------------------+
# | no|    name| age| gender|  occupation|       city| income|            property|
# +---+--------+----+-------+------------+-----------+-------+--------------------+
# |  1|  Pierre|  35|      M|       Staff|      Paris| 3500.0|                 car|
# |  2|  carol |  42|      F|     Officer|     Annecy| 4200.0|           car|house|
# |  3|     Teo|  30|   NULL|    Engineer|Bordeaux   | 9000.0|car|house|summerh...|
# |  4|Pauline |  29|      F|     Teacher|      Paris| 4200.0|                 car|
# |  5| Yasmine|  23|      F|Sales Person|      Lille| 4800.0|                 car|
# +---+--------+----+-------+------------+-----------+-------+--------------------+
# only showing top 5 rows

from pyspark.sql.functions import *


# 1 - Concat:
df_concat = df \
    .withColumn('occupation_city', concat(col(' occupation'), lit(' - '), (' city')))

df_concat.show(5)

# +---+--------+----+-------+------------+-----------+-------+--------------------+--------------------+
# | no|    name| age| gender|  occupation|       city| income|            property|     occupation_city|
# +---+--------+----+-------+------------+-----------+-------+--------------------+--------------------+
# |  1|  Pierre|  35|      M|       Staff|      Paris| 3500.0|                 car|       Staff - Paris|
# |  2|  carol |  42|      F|     Officer|     Annecy| 4200.0|           car|house|    Officer - Annecy|
# |  3|     Teo|  30|   NULL|    Engineer|Bordeaux   | 9000.0|car|house|summerh...|Engineer - Bordea...|
# |  4|Pauline |  29|      F|     Teacher|      Paris| 4200.0|                 car|     Teacher - Paris|
# |  5| Yasmine|  23|      F|Sales Person|      Lille| 4800.0|                 car|Sales Person - Lille|
# +---+--------+----+-------+------------+-----------+-------+--------------------+--------------------+
# only showing top 5 rows


# 2 - Number Format:

df_number_format = df \
    .withColumn('income_format', format_number(' income', 2))

df_number_format.show(5)

# +---+--------+----+-------+------------+-----------+-------+--------------------+-------------+
# | no|    name| age| gender|  occupation|       city| income|            property|income_format|
# +---+--------+----+-------+------------+-----------+-------+--------------------+-------------+
# |  1|  Pierre|  35|      M|       Staff|      Paris| 3500.0|                 car|     3,500.00|
# |  2|  carol |  42|      F|     Officer|     Annecy| 4200.0|           car|house|     4,200.00|
# |  3|     Teo|  30|   NULL|    Engineer|Bordeaux   | 9000.0|car|house|summerh...|     9,000.00|
# |  4|Pauline |  29|      F|     Teacher|      Paris| 4200.0|                 car|     4,200.00|
# |  5| Yasmine|  23|      F|Sales Person|      Lille| 4800.0|                 car|     4,800.00|
# +---+--------+----+-------+------------+-----------+-------+--------------------+-------------+
# only showing top 5 rows




# 3 - Lower, Initcap, Length:

df_lower =  df \
    .withColumn('occupation_lower', lower(col(' occupation'))) \
    .withColumn('name_initcap', initcap(col(' name'))) \
    .withColumn('city_length', length(col(' city')))

df_lower.show(5)
# +---+--------+----+-------+------------+-----------+-------+--------------------+----------------+------------+-----------+
# | no|    name| age| gender|  occupation|       city| income|            property|occupation_lower|name_initcap|city_length|
# +---+--------+----+-------+------------+-----------+-------+--------------------+----------------+------------+-----------+
# |  1|  Pierre|  35|      M|       Staff|      Paris| 3500.0|                 car|           staff|      Pierre|          5|
# |  2|  carol |  42|      F|     Officer|     Annecy| 4200.0|           car|house|         officer|      Carol |          6|
# |  3|     Teo|  30|   NULL|    Engineer|Bordeaux   | 9000.0|car|house|summerh...|        engineer|         Teo|         11|
# |  4|Pauline |  29|      F|     Teacher|      Paris| 4200.0|                 car|         teacher|    Pauline |          5|
# |  5| Yasmine|  23|      F|Sales Person|      Lille| 4800.0|                 car|    sales person|     Yasmine|          5|
# +---+--------+----+-------+------------+-----------+-------+--------------------+----------------+------------+-----------+
# only showing top 5 rows


# 4 - trim:

df_trim =  df \
    .withColumn('city_trim', trim(col(' city'))) \
    .withColumn('city_rtrim', rtrim(col(' city'))) \
    .withColumn('city_ltrim', ltrim(col(' city'))) \
    .withColumn('city_btrim', btrim(col(' city')))

df_trim.show(5)

df_trim \
    .withColumn('city_trim_length', length(col('city_trim'))) \
    .withColumn('city_rtrim_length', length(col('city_rtrim'))) \
    .withColumn('city_lrim_length', length(col('city_ltrim'))) \
    .withColumn('city_btrim_length', length(col('city_btrim'))).show(5)

# +---+--------+----+-------+------------+-----------+-------+--------------------+---------+----------+-----------+----------+----------------+-----------------+----------------+-----------------+
# | no|    name| age| gender|  occupation|       city| income|            property|city_trim|city_rtrim| city_ltrim|city_btrim|city_trim_length|city_rtrim_length|city_lrim_length|city_btrim_length|
# +---+--------+----+-------+------------+-----------+-------+--------------------+---------+----------+-----------+----------+----------------+-----------------+----------------+-----------------+
# |  1|  Pierre|  35|      M|       Staff|      Paris| 3500.0|                 car|    Paris|     Paris|      Paris|     Paris|               5|                5|               5|                5|
# |  2|  carol |  42|      F|     Officer|     Annecy| 4200.0|           car|house|   Annecy|    Annecy|     Annecy|    Annecy|               6|                6|               6|                6|
# |  3|     Teo|  30|   NULL|    Engineer|Bordeaux   | 9000.0|car|house|summerh...| Bordeaux|  Bordeaux|Bordeaux   |  Bordeaux|               8|                8|              11|                8|
# |  4|Pauline |  29|      F|     Teacher|      Paris| 4200.0|                 car|    Paris|     Paris|      Paris|     Paris|               5|                5|               5|                5|
# |  5| Yasmine|  23|      F|Sales Person|      Lille| 4800.0|                 car|    Lille|     Lille|      Lille|     Lille|               5|                5|               5|                5|
# +---+--------+----+-------+------------+-----------+-------+--------------------+---------+----------+-----------+----------+----------------+-----------------+----------------+-----------------+
# only showing top 5 rows

# 5 - replace, strip:

df_replace = df \
    .withColumn('city_PARis', regexp_replace(col(' city'), 'Par', 'PAR')) \
    .withColumn('property_split', split(col(' property'), '\\|')) \
    .withColumn('propert_firstvalue', col('property_split')[0])

df_replace.show(5)
# +---+--------+----+-------+------------+-----------+-------+--------------------+-----------+--------------------+------------------+
# | no|    name| age| gender|  occupation|       city| income|            property| city_PARis|      property_split|propert_firstvalue|
# +---+--------+----+-------+------------+-----------+-------+--------------------+-----------+--------------------+------------------+
# |  1|  Pierre|  35|      M|       Staff|      Paris| 3500.0|                 car|      PARis|               [car]|               car|
# |  2|  carol |  42|      F|     Officer|     Annecy| 4200.0|           car|house|     Annecy|        [car, house]|               car|
# |  3|     Teo|  30|   NULL|    Engineer|Bordeaux   | 9000.0|car|house|summerh...|Bordeaux   |[car, house, summ...|               car|
# |  4|Pauline |  29|      F|     Teacher|      Paris| 4200.0|                 car|      PARis|               [car]|               car|
# |  5| Yasmine|  23|      F|Sales Person|      Lille| 4800.0|                 car|      Lille|               [car]|               car|
# +---+--------+----+-------+------------+-----------+-------+--------------------+-----------+--------------------+------------------+
# only showing top 5 rows















































