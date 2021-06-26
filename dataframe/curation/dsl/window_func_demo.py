from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from Product import Product
import os.path
import yaml

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

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

    finFilePath = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/finances-small"
    financeDf = spark.read.parquet(finFilePath)
    financeDf.printSchema()

    accNumPrev4WindowSpec = Window.partitionBy("AccountNumber")\
        .orderBy("Date")\
        .rowsBetween(-4, 0)  # takes the first first 5 rows to window aggregation

    financeDf\
        .withColumn("Date", to_date(from_unixtime(unix_timestamp("Date", "MM/dd/yyyy"))))\
        .withColumn("RollingAvg", avg("Amount").over(accNumPrev4WindowSpec))\
        .show(20, False)

    accNumPrev4WindowSpecUb = Window.partitionBy("AccountNumber")\
        .orderBy("Date")\
        .rowsBetween(Window.unboundedPreceding, 0)  #

    financeDf\
        .withColumn("Date", to_date(from_unixtime(unix_timestamp("Date", "MM/dd/yyyy"))))\
        .withColumn("RollingAvg", avg("Amount").over(accNumPrev4WindowSpec))\
        .withColumn("CumulativeSum", sum("Amount").over(accNumPrev4WindowSpec))\
        .show(20, False)

    productList = [
        Product("Thin", "Cell phone", 6000),
        Product("Normal", "Tablet", 1500),
        Product("Mini", "Tablet", 5500),
        Product("Ultra Thin", "Cell phone", 5000),
        Product("Very Thin", "Cell phone", 6000),
        Product("Big", "Tablet", 2500),
        Product("Bendable", "Cell phone", 3000),
        Product("Foldable", "Cell phone", 3000),
        Product("Pro", "Tablet", 4500),
        Product("Pro2", "Tablet", 6500)
    ]

    products = spark.createDataFrame(productList)
    products.printSchema()

    catRevenueWindowSpec = Window.partitionBy("category")\
        .orderBy("revenue")

    products \
        .select("product",
                "category",
                "revenue",
                lag("revenue", 1).over(catRevenueWindowSpec).alias("prevRevenue"),
                lag("revenue", 2, 0).over(catRevenueWindowSpec).alias("prev2Revenue"),
                row_number().over(catRevenueWindowSpec).alias("row_number"),
                rank().over(catRevenueWindowSpec).alias("rev_rank"),
                dense_rank().over(catRevenueWindowSpec).alias("rev_dense_rank")) \
        .show()

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/dsl/window_func_demo.py
# spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.4 window_func_demo.py
# For windows spark 3.0 you need to add folowing line: spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

'''
root
 |-- AccountNumber: string (nullable = true)
 |-- Amount: double (nullable = true)
 |-- Date: string (nullable = true)
 |-- Description: string (nullable = true)

+-------------+------+----------+----------------------------------+------------------+
|AccountNumber|Amount|Date      |Description                       |RollingAvg        |
+-------------+------+----------+----------------------------------+------------------+
|456-DEF-456  |200.0 |2015-01-03|Electronics                       |200.0             |
|456-DEF-456  |23.16 |2015-01-11|Unknown                           |111.58            |
|456-DEF-456  |20.0  |2015-01-12|Electronics                       |81.05333333333333 |
|456-DEF-456  |78.77 |2015-01-24|Grocery Store                     |80.4825           |
|456-DEF-456  |93.71 |2015-01-31|Grocery Store                     |83.128            |
|456-DEF-456  |108.64|2015-01-31|Park                              |64.856            |
|456-DEF-456  |116.11|2015-01-31|Books                             |83.446            |
|456-DEF-456  |18.99 |2015-02-12|Drug Store                        |83.244            |
|456-DEF-456  |215.67|2015-02-20|Electronics                       |110.624           |
|456-DEF-456  |71.31 |2015-02-22|Gas                               |106.144           |
|456-DEF-456  |6.78  |2015-02-23|Drug Store                        |85.77199999999999 |
|456-DEF-456  |189.53|2015-03-02|Books                             |100.45599999999999|
|456-DEF-456  |281.0 |2015-03-09|Electronics                       |152.858           |
|456-DEF-456  |42.85 |2015-03-12|Gas                               |118.29400000000001|
|333-XYZ-999  |106.0 |2015-01-04|Gas                               |106.0             |
|333-XYZ-999  |52.13 |2015-01-17|Gas                               |79.065            |
|333-XYZ-999  |41.67 |2015-01-19|Some Totally Fake Long Description|66.60000000000001 |
|333-XYZ-999  |56.37 |2015-01-21|Gas                               |64.0425           |
|333-XYZ-999  |86.24 |2015-01-29|Movies                            |68.482            |
|333-XYZ-999  |131.04|2015-02-11|Electronics                       |73.49000000000001 |
+-------------+------+----------+----------------------------------+------------------+
only showing top 20 rows

+-------------+------+----------+----------------------------------+------------------+------------------+
|AccountNumber|Amount|Date      |Description                       |RollingAvg        |CumulativeSum     |
+-------------+------+----------+----------------------------------+------------------+------------------+
|456-DEF-456  |200.0 |2015-01-03|Electronics                       |200.0             |200.0             |
|456-DEF-456  |23.16 |2015-01-11|Unknown                           |111.58            |223.16            |
|456-DEF-456  |20.0  |2015-01-12|Electronics                       |81.05333333333333 |243.16            |
|456-DEF-456  |78.77 |2015-01-24|Grocery Store                     |80.4825           |321.93            |
|456-DEF-456  |93.71 |2015-01-31|Grocery Store                     |83.128            |415.64            |
|456-DEF-456  |108.64|2015-01-31|Park                              |64.856            |324.28            |
|456-DEF-456  |116.11|2015-01-31|Books                             |83.446            |417.23            |
|456-DEF-456  |18.99 |2015-02-12|Drug Store                        |83.244            |416.22            |
|456-DEF-456  |215.67|2015-02-20|Electronics                       |110.624           |553.12            |
|456-DEF-456  |71.31 |2015-02-22|Gas                               |106.144           |530.72            |
|456-DEF-456  |6.78  |2015-02-23|Drug Store                        |85.77199999999999 |428.85999999999996|
|456-DEF-456  |189.53|2015-03-02|Books                             |100.45599999999999|502.28            |
|456-DEF-456  |281.0 |2015-03-09|Electronics                       |152.858           |764.29            |
|456-DEF-456  |42.85 |2015-03-12|Gas                               |118.29400000000001|591.47            |
|333-XYZ-999  |106.0 |2015-01-04|Gas                               |106.0             |106.0             |
|333-XYZ-999  |52.13 |2015-01-17|Gas                               |79.065            |158.13            |
|333-XYZ-999  |41.67 |2015-01-19|Some Totally Fake Long Description|66.60000000000001 |199.8             |
|333-XYZ-999  |56.37 |2015-01-21|Gas                               |64.0425           |256.17            |
|333-XYZ-999  |86.24 |2015-01-29|Movies                            |68.482            |342.41            |
|333-XYZ-999  |131.04|2015-02-11|Electronics                       |73.49000000000001 |367.45000000000005|
+-------------+------+----------+----------------------------------+------------------+------------------+
only showing top 20 rows

root
 |-- category: string (nullable = true)
 |-- product: string (nullable = true)
 |-- revenue: long (nullable = true)

+----------+----------+-------+-----------+------------+----------+--------+--------------+
|   product|  category|revenue|prevRevenue|prev2Revenue|row_number|rev_rank|rev_dense_rank|
+----------+----------+-------+-----------+------------+----------+--------+--------------+
|  Bendable|Cell phone|   3000|       null|           0|         1|       1|             1|
|  Foldable|Cell phone|   3000|       3000|           0|         2|       1|             1|
|Ultra Thin|Cell phone|   5000|       3000|        3000|         3|       3|             2|
|      Thin|Cell phone|   6000|       5000|        3000|         4|       4|             3|
| Very Thin|Cell phone|   6000|       6000|        5000|         5|       4|             3|
|    Normal|    Tablet|   1500|       null|           0|         1|       1|             1|
|       Big|    Tablet|   2500|       1500|           0|         2|       2|             2|
|       Pro|    Tablet|   4500|       2500|        1500|         3|       3|             3|
|      Mini|    Tablet|   5500|       4500|        2500|         4|       4|             4|
|      Pro2|    Tablet|   6500|       5500|        4500|         5|       5|             5|
+----------+----------+-------+-----------+------------+----------+--------+--------------+

'''