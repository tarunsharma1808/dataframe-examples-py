from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import os.path
import yaml

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()
    # .master('local[*]') \
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    print("\nCreating dataframe ingestion parquet file using 'SparkSession.read.parquet()',")
    nyc_omo_df = spark.read \
        .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/NYC_OMO") \
        .repartition(5)

    print("# of records = " + str(nyc_omo_df.count()))
    print("# of partitions = " + str(nyc_omo_df.rdd.getNumPartitions))

    nyc_omo_df.printSchema()

    print("Summery of NYC Open Market Order (OMO) charges dataset,")
    nyc_omo_df.describe().show()

    print("OMO frequency distribution of different Boroughs,")
    nyc_omo_df.groupBy("Boro") \
        .agg({"Boro": "count"}) \
        .withColumnRenamed("count(Boro)", "OrderFrequency") \
        .show()

    print("OMO's Zip & Borough list,")

    boro_zip_df = nyc_omo_df \
        .select("Boro", nyc_omo_df["Zip"].cast(IntegerType())) \
        .groupBy("Boro") \
        .agg({"Zip": "collect_set"}) \
        .withColumnRenamed("collect_set(Zip)", "ZipList") \
        .withColumn("ZipCount", F.size("ZipList"))

    boro_zip_df \
        .select("Boro", "ZipCount", "ZipList") \
        .show(5)

    # Window functions
    window_spec = Window.partitionBy("OMOCreateDate")
    omo_daily_freq = nyc_omo_df \
        .withColumn("OMODailyFreq", F.count("OMOID").over(window_spec))

    print("# of partitions in window'ed OM dataframe = " + str(omo_daily_freq.count()))
    omo_daily_freq.show(5)

    omo_daily_freq.select("OMOCreateDate", "OMODailyFreq") \
        .distinct() \
        .show(5)

    omo_daily_freq \
        .repartition(5) \
        .write \
        .mode("overwrite") \
        .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/nyc_omo_data")

    spark.stop()

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/ingestion/files/parquet_df.py

'''
root
 |-- OMOID: integer (nullable = true)
 |-- OMONumber: string (nullable = true)
 |-- BuildingID: integer (nullable = true)
 |-- BoroID: integer (nullable = true)
 |-- Boro: string (nullable = true)
 |-- HouseNumber: string (nullable = true)
 |-- StreetName: string (nullable = true)
 |-- Apartment: string (nullable = true)
 |-- Zip: double (nullable = true)
 |-- Block: integer (nullable = true)
 |-- Lot: integer (nullable = true)
 |-- LifeCycle: string (nullable = true)
 |-- WorkTypeGeneral: string (nullable = true)
 |-- OMOStatusReason: string (nullable = true)
 |-- OMOAwardAmount: double (nullable = true)
 |-- OMOCreateDate: timestamp (nullable = true)
 |-- NetChangeOrders: integer (nullable = true)
 |-- OMOAwardDate: timestamp (nullable = true)
 |-- IsAEP: string (nullable = true)
 |-- IsCommercialDemolition: string (nullable = true)
 |-- ServiceChargeFlag: boolean (nullable = true)
 |-- FEMAEventID: integer (nullable = true)
 |-- FEMAEvent: string (nullable = true)
 |-- OMODescription: string (nullable = true)

Summery of NYC Open Market Order (OMO) charges dataset,
+-------+------------------+---------+------------------+------------------+-------------+------------------+--------------+--------------------+------------------+------------------+------------------+---------------+---------------+--------------------+------------------+--------------------+-----+----------------------+------------------+---------------+--------------------+
|summary|             OMOID|OMONumber|        BuildingID|            BoroID|         Boro|       HouseNumber|    StreetName|           Apartment|               Zip|             Block|               Lot|      LifeCycle|WorkTypeGeneral|     OMOStatusReason|    OMOAwardAmount|     NetChangeOrders|IsAEP|IsCommercialDemolition|       FEMAEventID|      FEMAEvent|      OMODescription|
+-------+------------------+---------+------------------+------------------+-------------+------------------+--------------+--------------------+------------------+------------------+------------------+---------------+---------------+--------------------+------------------+--------------------+-----+----------------------+------------------+---------------+--------------------+
|  count|            406023|   406023|            406023|            406023|       406023|            406023|        406023|              309911|            405956|            406023|            406023|         406023|         406023|              402081|            406023|              406023|24190|                   704|              1183|           1069|              406021|
|   mean|   2283145.1962278|     null|254493.45723518127|2.5287853151176165|         null|1056.3171764501242|          null|6.471340052741421E98|10816.503759028072|3443.0685700071176|121.48553899655931|           null|           null|                null|1612.8404196807014|0.013314516665312064| null|                  null|330.73034657650044|           null|                 1.0|
| stddev|1213798.1559188957|     null|222068.89587673222|0.9435178601177172|         null|1164.6251654741616|          null|2.275318009024913...| 502.0975625995414|2623.8266035984743|    758.1596606927|           null|           null|                null|18894.063675983285|   7.739198149613587| null|                  null|108.04912471059545|           null|                 NaN|
|    min|                28|  D000001|                 1|                 1|        Bronx|                 0|      1 AVENUE|                    |               0.0|                 0|                 0|       Building|           7AFA|         Apt. Vacant|               0.0|                   0|  AEP|            COMM DEMOL|                 0|Hurricane Sandy|                    |
|    max|           4758368|  EJ15477|            989058|                 5|Staten Island|               999|ZULETTE AVENUE|                rubb|           11697.0|             16350|              9100|UnderConstructi|           UTIL|landlord Restored...|         4150000.0|                4906|  AEP|            COMM DEMOL|               366|Hurricane Sandy|â€œsandyâ€� damag...|
+-------+------------------+---------+------------------+------------------+-------------+------------------+--------------+--------------------+------------------+------------------+------------------+---------------+---------------+--------------------+------------------+--------------------+-----+----------------------+------------------+---------------+--------------------+

OMO frequency distribution of different Boroughs,
+-------------+--------------+
|         Boro|OrderFrequency|
+-------------+--------------+
|     Brooklyn|        180356|
|    Manhattan|         67738|
|        Bronx|        110676|
|       Queens|         39678|
|Staten Island|          7575|
+-------------+--------------+

OMO's Zip & Borough list,
+-------------+--------+--------------------+
|         Boro|ZipCount|             ZipList|
+-------------+--------+--------------------+
|     Brooklyn|      39|[11209, 11239, 11...|
|    Manhattan|      45|[0, 10040, 10019,...|
|        Bronx|      26|[10461, 10455, 10...|
|       Queens|      62|[11412, 11362, 11...|
|Staten Island|      12|[10312, 10309, 10...|
+-------------+--------+--------------------+

# of partitions in window'ed OM dataframe = 406023
+-----+---------+----------+------+--------+-----------+----------------+---------+-------+-----+----+---------+---------------+-----------------+--------------+-------------------+---------------+-------------------+-----+----------------------+-----------------+-----------+---------+--------------------+------------+
|OMOID|OMONumber|BuildingID|BoroID|    Boro|HouseNumber|      StreetName|Apartment|    Zip|Block| Lot|LifeCycle|WorkTypeGeneral|  OMOStatusReason|OMOAwardAmount|      OMOCreateDate|NetChangeOrders|       OMOAwardDate|IsAEP|IsCommercialDemolition|ServiceChargeFlag|FEMAEventID|FEMAEvent|      OMODescription|OMODailyFreq|
+-----+---------+----------+------+--------+-----------+----------------+---------+-------+-----+----+---------+---------------+-----------------+--------------+-------------------+---------------+-------------------+-----+----------------------+-----------------+-----------+---------+--------------------+------------+
|  620|  E000473|    208595|     3|Brooklyn|       1964|   BERGEN STREET|     null|11233.0| 1453|  19| Building|             GC|Landlord Complied|           0.0|1998-08-24 18:30:00|              0|               null| null|                  null|            false|       null|     null|replace defective...|           9|
|  605|  E000465|    355144|     3|Brooklyn|       1140|PRESIDENT STREET|     null|11225.0| 1282|  14| Building|          PLUMB|   Refused Access|          35.0|1998-08-24 18:30:00|              0|1998-09-22 18:30:00| null|                  null|             true|       null|     null|unclogg 4'' waste...|           9|
|  608|  E000468|    632345|     4|  Queens|        208| BEACH 42 STREET|        1|11691.0|15850|  31| Building|          PLUMB|    OMO Completed|        2600.0|1998-08-24 18:30:00|              0|1998-09-01 18:30:00| null|                  null|            false|       null|     null|replace water mai...|           9|
|  603|  E000463|    349504|     3|Brooklyn|       1284|  PACIFIC STREET|     null|11216.0| 1207|  17| Building|           ELEC|    OMO Completed|        1745.0|1998-08-24 18:30:00|              0|1998-09-02 18:30:00| null|                  null|            false|       null|     null|obtain all necess...|           9|
|  621|  E000474|    373691|     3|Brooklyn|       1359|  ST JOHNS PLACE|     null|11213.0| 1378|7501| Building|           IRON|    OMO Completed|         297.0|1998-08-24 18:30:00|              0|1998-09-01 18:30:00| null|                  null|            false|       null|     null|re-install existi...|           9|
+-----+---------+----------+------+--------+-----------+----------------+---------+-------+-----+----+---------+---------------+-----------------+--------------+-------------------+---------------+-------------------+-----+----------------------+-----------------+-----------+---------+--------------------+------------+
only showing top 5 rows

+-------------------+------------+
|      OMOCreateDate|OMODailyFreq|
+-------------------+------------+
|1998-08-24 18:30:00|           9|
|1998-09-07 18:30:00|           5|
|1998-12-07 18:30:00|          14|
|1998-12-15 18:30:00|          20|
|1998-12-17 18:30:00|           6|
+-------------------+------------+
only showing top 5 rows

'''