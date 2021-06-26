from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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

    fin_file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/finances-small"
    finance_df = spark.read.parquet(fin_file_path)

    finance_df.printSchema()
    finance_df.show(5, False)



    finance_df\
        .orderBy("Amount")\
        .show(5)

    # concat_ws function available sql.functions
    finance_df\
        .select(concat_ws(" - ", "AccountNumber", "Description").alias("AccountDetails"))\
        .show(5, False)

    finance_df\
        .withColumn("AccountDetails", concat_ws(" - ", "AccountNumber", "Description"))\
        .show(5, False)

    agg_finance_df = finance_df\
        .groupBy("AccountNumber")\
        .agg(avg("Amount").alias("AverageTransaction"),
             sum("Amount").alias("TotalTransaction"),
             count("Amount").alias("NumberOfTransaction"),
             max("Amount").alias("MaxTransaction"),
             min("Amount").alias("MinTransaction"),
             collect_set("Description").alias("UniqueTransactionDescriptions")
        )

    agg_finance_df.show(5, False)

    agg_finance_df\
        .select("AccountNumber",
                "UniqueTransactionDescriptions",
                size("UniqueTransactionDescriptions").alias("CountOfUniqueTransactionTypes"),
                sort_array("UniqueTransactionDescriptions", False).alias("OrderedUniqueTransactionDescriptions"),
                array_contains("UniqueTransactionDescriptions", "Movies").alias("WentToMovies"))\
        .show(5, False)

    companies_df = spark.read.json("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/company.json")
    print("Count = ", companies_df.count())
    companies_df.show(5, False)
    companies_df.printSchema()

    employee_df_temp = companies_df.select("company", explode("employees").alias("employee"))
    employee_df_temp.show()

    companies_df \
        .select("company", posexplode("employees").alias("employeePosition", "employee")) \
        .show()

    employeeDf = employee_df_temp.select("company", expr("employee.firstName as firstName"))
    employeeDf.select("*",
                      when(col("company") == "FamilyCo", "Premium")
                      .when(col("company") == "OldCo", "Legacy")
                      .otherwise("Standard").alias("Tier"))\
                .show(5, False)

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/dsl/finance_data_analysis.py
# spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.4 finance_data_analysis.py

'''
root
 |-- AccountNumber: string (nullable = true)
 |-- Amount: double (nullable = true)
 |-- Date: string (nullable = true)
 |-- Description: string (nullable = true)

+-------------+------+---------+-------------+
|AccountNumber|Amount|Date     |Description  |
+-------------+------+---------+-------------+
|123-ABC-789  |1.23  |1/1/2015 |Drug Store   |
|456-DEF-456  |200.0 |1/3/2015 |Electronics  |
|333-XYZ-999  |106.0 |1/4/2015 |Gas          |
|123-ABC-789  |2.36  |1/9/2015 |Grocery Store|
|456-DEF-456  |23.16 |1/11/2015|Unknown      |
+-------------+------+---------+-------------+
only showing top 5 rows
# orderBy("Amount")
+-------------+------+---------+-------------+
|AccountNumber|Amount|     Date|  Description|
+-------------+------+---------+-------------+
|  123-ABC-789|   0.0|2/14/2015|      Unknown|
|  123-ABC-789|  1.23| 1/1/2015|   Drug Store|
|  987-CBA-321|  2.29|1/31/2015|   Drug Store|
|  123-ABC-789|  2.36| 1/9/2015|Grocery Store|
|  456-DEF-456|  6.78|2/23/2015|   Drug Store|
+-------------+------+---------+-------------+
only showing top 5 rows

concat_ws + select()
+---------------------------+
|AccountDetails             |
+---------------------------+
|123-ABC-789 - Drug Store   |
|456-DEF-456 - Electronics  |
|333-XYZ-999 - Gas          |
|123-ABC-789 - Grocery Store|
|456-DEF-456 - Unknown      |
+---------------------------+
only showing top 5 rows

concat_ws + withColumn -> Same as above but as expected, other columns will also be visible
+-------------+------+---------+-------------+---------------------------+
|AccountNumber|Amount|Date     |Description  |AccountDetails             |
+-------------+------+---------+-------------+---------------------------+
|123-ABC-789  |1.23  |1/1/2015 |Drug Store   |123-ABC-789 - Drug Store   |
|456-DEF-456  |200.0 |1/3/2015 |Electronics  |456-DEF-456 - Electronics  |
|333-XYZ-999  |106.0 |1/4/2015 |Gas          |333-XYZ-999 - Gas          |
|123-ABC-789  |2.36  |1/9/2015 |Grocery Store|123-ABC-789 - Grocery Store|
|456-DEF-456  |23.16 |1/11/2015|Unknown      |456-DEF-456 - Unknown      |
+-------------+------+---------+-------------+---------------------------+
only showing top 5 rows

+-------------+------------------+------------------+-------------------+--------------+--------------+------------------------------------------------------------------------------------+
|AccountNumber|AverageTransaction|TotalTransaction  |NumberOfTransaction|MaxTransaction|MinTransaction|UniqueTransactionDescriptions                                                       |
+-------------+------------------+------------------+-------------------+--------------+--------------+------------------------------------------------------------------------------------+
|123-ABC-789  |362.9785714285714 |5081.7            |14                 |4000.0        |0.0           |[Electronics, Grocery Store, Unknown, Park, Drug Store, Movies]                     |
|333-XYZ-999  |104.09833333333334|1249.18           |12                 |241.8         |41.67         |[Electronics, Grocery Store, Books, Some Totally Fake Long Description, Gas, Movies]|
|456-DEF-456  |104.75142857142855|1466.5199999999998|14                 |281.0         |6.78          |[Electronics, Grocery Store, Books, Unknown, Park, Drug Store, Gas]                 |
|987-CBA-321  |96.87888888888887 |871.9099999999999 |9                  |267.93        |2.29          |[Electronics, Grocery Store, Books, Park, Drug Store, Gas, Movies]                  |
+-------------+------------------+------------------+-------------------+--------------+--------------+------------------------------------------------------------------------------------+

+-------------+------------------------------------------------------------------------------------+-----------------------------+------------------------------------------------------------------------------------+------------+
|AccountNumber|UniqueTransactionDescriptions                                                       |CountOfUniqueTransactionTypes|OrderedUniqueTransactionDescriptions                                                |WentToMovies|
+-------------+------------------------------------------------------------------------------------+-----------------------------+------------------------------------------------------------------------------------+------------+
|123-ABC-789  |[Electronics, Grocery Store, Unknown, Park, Drug Store, Movies]                     |6                            |[Unknown, Park, Movies, Grocery Store, Electronics, Drug Store]                     |true        |
|333-XYZ-999  |[Electronics, Grocery Store, Books, Some Totally Fake Long Description, Gas, Movies]|6                            |[Some Totally Fake Long Description, Movies, Grocery Store, Gas, Electronics, Books]|true        |
|456-DEF-456  |[Electronics, Grocery Store, Books, Unknown, Park, Drug Store, Gas]                 |7                            |[Unknown, Park, Grocery Store, Gas, Electronics, Drug Store, Books]                 |false       |
|987-CBA-321  |[Electronics, Grocery Store, Books, Park, Drug Store, Gas, Movies]                  |7                            |[Park, Movies, Grocery Store, Gas, Electronics, Drug Store, Books]                  |true        |
+-------------+------------------------------------------------------------------------------------+-----------------------------+------------------------------------------------------------------------------------+------------+

Count =  4
+--------+-------------------------------------+
|company |employees                            |
+--------+-------------------------------------+
|NewCo   |[[Sidhartha, Ray], [Pratik, Solanki]]|
|FamilyCo|[[Jiten, Gupta], [Pallavi, Gupta]]   |
|OldCo   |[[Vivek, Garg], [Nitin, Gupta]]      |
|ClosedCo|[]                                   |
+--------+-------------------------------------+

root
 |-- company: string (nullable = true)
 |-- employees: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- firstName: string (nullable = true)
 |    |    |-- lastName: string (nullable = true)

explode("employees")
+--------+-----------------+
| company|         employee|
+--------+-----------------+
|   NewCo| [Sidhartha, Ray]|
|   NewCo|[Pratik, Solanki]|
|FamilyCo|   [Jiten, Gupta]|
|FamilyCo| [Pallavi, Gupta]|
|   OldCo|    [Vivek, Garg]|
|   OldCo|   [Nitin, Gupta]|
+--------+-----------------+
posexplode
+--------+----------------+-----------------+
| company|employeePosition|         employee|
+--------+----------------+-----------------+
|   NewCo|               0| [Sidhartha, Ray]|
|   NewCo|               1|[Pratik, Solanki]|
|FamilyCo|               0|   [Jiten, Gupta]|
|FamilyCo|               1| [Pallavi, Gupta]|
|   OldCo|               0|    [Vivek, Garg]|
|   OldCo|               1|   [Nitin, Gupta]|
+--------+----------------+-----------------+

+--------+---------+--------+
|company |firstName|Tier    |
+--------+---------+--------+
|NewCo   |Sidhartha|Standard|
|NewCo   |Pratik   |Standard|
|FamilyCo|Jiten    |Premium |
|FamilyCo|Pallavi  |Premium |
|OldCo   |Vivek    |Legacy  |
+--------+---------+--------+
only showing top 5 rows

'''