from pyspark.sql import SparkSession
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
    app_config_path = os.path.abspath(current_dir + "/../../../"+"application.yml")
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
    finance_df = spark.sql("select * from parquet.`{}`".format(fin_file_path))

    finance_df.printSchema()
    finance_df.show(5, False)
    finance_df.createOrReplaceTempView("finances")

    spark.sql("select * from finances order by amount").show(5, False)

    spark.sql("select concat_ws(' - ', AccountNumber, Description) as AccountDetails from finances").show(5, False)

    agg_finance_df = spark.sql("""
        select
            AccountNumber,
            sum(Amount) as TotalTransaction,
            count(Amount) as NumberOfTransaction,
            max(Amount) as MaxTransaction,
            min(Amount) as MinTransaction,
            collect_set(Description) as UniqueTransactionDescriptions
        from
            finances
        group by
            AccountNumber
        """)

    agg_finance_df.show(5, False)
    agg_finance_df.createOrReplaceTempView("agg_finances")

    spark.sql(app_conf["spark_sql_demo"]["agg_demo"]) \
        .show(5, False)

    companies_df = spark.read.json("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/company.json")
    companies_df.createOrReplaceTempView("companies")
    companies_df.show(5, False)
    companies_df.printSchema()

    employee_df_temp = spark.sql("select company, explode(employees) as employee from companies")
    employee_df_temp.show()
    employee_df_temp.createOrReplaceTempView("employees")
    spark.sql("select company, posexplode(employees) as (employeePosition, employee) from companies") \
        .show()

    spark.sql(app_conf["spark_sql_demo"]["case_when_demo"]) \
        .show(5, False)

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/sql/finance_data_analysis.py

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

+-------------+------+---------+-------------+
|AccountNumber|Amount|Date     |Description  |
+-------------+------+---------+-------------+
|123-ABC-789  |0.0   |2/14/2015|Unknown      |
|123-ABC-789  |1.23  |1/1/2015 |Drug Store   |
|987-CBA-321  |2.29  |1/31/2015|Drug Store   |
|123-ABC-789  |2.36  |1/9/2015 |Grocery Store|
|456-DEF-456  |6.78  |2/23/2015|Drug Store   |
+-------------+------+---------+-------------+
only showing top 5 rows

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

+-------------+------------------+-------------------+--------------+--------------+------------------------------------------------------------------------------------+
|AccountNumber|TotalTransaction  |NumberOfTransaction|MaxTransaction|MinTransaction|UniqueTransactionDescriptions                                                       |
+-------------+------------------+-------------------+--------------+--------------+------------------------------------------------------------------------------------+
|123-ABC-789  |5081.7            |14                 |4000.0        |0.0           |[Electronics, Grocery Store, Unknown, Park, Drug Store, Movies]                     |
|333-XYZ-999  |1249.18           |12                 |241.8         |41.67         |[Electronics, Grocery Store, Books, Some Totally Fake Long Description, Gas, Movies]|
|456-DEF-456  |1466.5199999999998|14                 |281.0         |6.78          |[Electronics, Grocery Store, Books, Unknown, Park, Drug Store, Gas]                 |
|987-CBA-321  |871.9099999999999 |9                  |267.93        |2.29          |[Electronics, Grocery Store, Books, Park, Drug Store, Gas, Movies]                  |
+-------------+------------------+-------------------+--------------+--------------+------------------------------------------------------------------------------------+

+-------------+------------------------------------------------------------------------------------+------------------------------------------------------------------------------------+-----------------------------+------------+
|AccountNumber|UniqueTransactionDescriptions                                                       |OrderedUniqueTransactionDescriptions                                                |CountOfUniqueTransactionTypes|WentToMovies|
+-------------+------------------------------------------------------------------------------------+------------------------------------------------------------------------------------+-----------------------------+------------+
|123-ABC-789  |[Electronics, Grocery Store, Unknown, Park, Drug Store, Movies]                     |[Unknown, Park, Movies, Grocery Store, Electronics, Drug Store]                     |6                            |true        |
|333-XYZ-999  |[Electronics, Grocery Store, Books, Some Totally Fake Long Description, Gas, Movies]|[Some Totally Fake Long Description, Movies, Grocery Store, Gas, Electronics, Books]|6                            |true        |
|456-DEF-456  |[Electronics, Grocery Store, Books, Unknown, Park, Drug Store, Gas]                 |[Unknown, Park, Grocery Store, Gas, Electronics, Drug Store, Books]                 |7                            |false       |
|987-CBA-321  |[Electronics, Grocery Store, Books, Park, Drug Store, Gas, Movies]                  |[Park, Movies, Grocery Store, Gas, Electronics, Drug Store, Books]                  |7                            |true        |
+-------------+------------------------------------------------------------------------------------+------------------------------------------------------------------------------------+-----------------------------+------------+

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