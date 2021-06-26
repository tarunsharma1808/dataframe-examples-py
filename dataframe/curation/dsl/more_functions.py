from pyspark.sql import SparkSession
from pyspark.sql.functions import first,trim,lower,ltrim,initcap,format_string,coalesce,lit,col
from Person import Person

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    people_df = spark.createDataFrame([
        Person("Sidhartha", "Ray", 32, None, "Programmer"),
        Person("Pratik", "Solanki", 22, 176.7, None),
        Person("Ashok ", "Pradhan", 62, None, None),
        Person(" ashok", "Pradhan", 42, 125.3, "Chemical Engineer"),
        Person("Pratik", "Solanki", 22, 222.2, "Teacher")
    ])

    people_df.show()
    # people_df.groupBy("firstName").agg(first("weightInLbs")).show()
    '''
    +---------+------------------+
    |firstName|first(weightInLbs)|
    +---------+------------------+
    |   Ashok |              null|
    |Sidhartha|              null|
    |   Pratik|             176.7|
    |    ashok|             125.3|
    +---------+------------------+
    '''
    # people_df.groupBy(trim(lower(col('firstName')))).agg(first("weightInLbs")).show()
    '''
    +----------------------+------------------+
    |trim(lower(firstName))|first(weightInLbs)|
    +----------------------+------------------+
    |             sidhartha|              null|
    |                pratik|             176.7|
    |                 ashok|              null|
    +----------------------+------------------+
    '''
    # people_df.groupBy(trim(lower(col("firstName")))).agg(first("weightInLbs", True)).show()
    '''
    +----------------------+------------------+
    |trim(lower(firstName))|first(weightInLbs)|
    +----------------------+------------------+
    |             sidhartha|              null|
    |                pratik|             176.7|
    |                 ashok|             125.3|
    +----------------------+------------------+
    '''
    # people_df.sort(col("weightInLbs").desc()).groupBy(trim(lower(col("firstName")))).agg(first("weightInLbs", True)).show()
    '''
    +----------------------+------------------+
    |trim(lower(firstName))|first(weightInLbs)|
    +----------------------+------------------+
    |             sidhartha|              null|
    |                pratik|             222.2|
    |                 ashok|             125.3|
    +----------------------+------------------+
    '''
    # people_df.sort(col("weightInLbs").asc_nulls_last()).groupBy(trim(lower(col("firstName")))).agg(first("weightInLbs", True)).show()
    '''
    +---------+------------------+
    |firstName|first(weightInLbs)|
    +---------+------------------+
    |Sidhartha|              null|
    |    Ashok|              null|
    |   Pratik|             176.7|
    +---------+------------------+
    '''

    corrected_people_df = people_df\
        .withColumn("firstName", initcap("firstName"))\
        .withColumn("firstName", ltrim(initcap("firstName")))\
        .withColumn("firstName", trim(initcap("firstName")))

    corrected_people_df.groupBy("firstName").agg(first("weightInLbs")).show()
    '''
    +---------+------------------+
    |firstName|first(weightInLbs)|
    +---------+------------------+
    |Sidhartha|              null|
    |    Ashok|              null|
    |   Pratik|             176.7|
    +---------+------------------+
    '''

    corrected_people_df = corrected_people_df\
        .withColumn("fullName", format_string("%s %s", "firstName", "lastName"))

    corrected_people_df.show()
    '''
    +---+---------+-----------------+--------+-----------+--------------+
    |age|firstName|          jobType|lastName|weightInLbs|      fullName|
    +---+---------+-----------------+--------+-----------+--------------+
    | 32|Sidhartha|       Programmer|     Ray|       null| Sidhartha Ray|
    | 22|   Pratik|             null| Solanki|      176.7|Pratik Solanki|
    | 62|    Ashok|             null| Pradhan|       null| Ashok Pradhan|
    | 42|    Ashok|Chemical Engineer| Pradhan|      125.3| Ashok Pradhan|
    | 22|   Pratik|          Teacher| Solanki|      222.2|Pratik Solanki|
    +---+---------+-----------------+--------+-----------+--------------+
    '''

    corrected_people_df = corrected_people_df\
        .withColumn("weightInLbs", coalesce("weightInLbs", lit(0)))

    corrected_people_df.show()
    '''
    +---+---------+-----------------+--------+-----------+--------------+
    |age|firstName|          jobType|lastName|weightInLbs|      fullName|
    +---+---------+-----------------+--------+-----------+--------------+
    | 32|Sidhartha|       Programmer|     Ray|        0.0| Sidhartha Ray|
    | 22|   Pratik|             null| Solanki|      176.7|Pratik Solanki|
    | 62|    Ashok|             null| Pradhan|        0.0| Ashok Pradhan|
    | 42|    Ashok|Chemical Engineer| Pradhan|      125.3| Ashok Pradhan|
    | 22|   Pratik|          Teacher| Solanki|      222.2|Pratik Solanki|
    +---+---------+-----------------+--------+-----------+--------------+
    '''

    corrected_people_df\
        .filter(lower(col("jobType")).contains("engineer"))\
        .show()
    '''
    +---+---------+-----------------+--------+-----------+-------------+
    |age|firstName|          jobType|lastName|weightInLbs|     fullName|
    +---+---------+-----------------+--------+-----------+-------------+
    | 42|    Ashok|Chemical Engineer| Pradhan|      125.3|Ashok Pradhan|
    +---+---------+-----------------+--------+-----------+-------------+
    '''


    # List
    corrected_people_df \
        .filter(lower(col("jobType")).isin(["chemical engineer", "abc", "teacher"])) \
        .show()
    '''
    +---+---------+-----------------+--------+-----------+--------------+
    |age|firstName|          jobType|lastName|weightInLbs|      fullName|
    +---+---------+-----------------+--------+-----------+--------------+
    | 42|    Ashok|Chemical Engineer| Pradhan|      125.3| Ashok Pradhan|
    | 22|   Pratik|          Teacher| Solanki|      222.2|Pratik Solanki|
    +---+---------+-----------------+--------+-----------+--------------+
    '''

    # Without List
    corrected_people_df\
        .filter(lower(col("jobType")).isin("chemical engineer", "teacher"))\
        .show()
    '''
    +---+---------+-----------------+--------+-----------+--------------+
    |age|firstName|          jobType|lastName|weightInLbs|      fullName|
    +---+---------+-----------------+--------+-----------+--------------+
    | 42|    Ashok|Chemical Engineer| Pradhan|      125.3| Ashok Pradhan|
    | 22|   Pratik|          Teacher| Solanki|      222.2|Pratik Solanki|
    +---+---------+-----------------+--------+-----------+--------------+
    '''

    # Exclusion
    corrected_people_df \
        .filter(~lower(col("jobType")).isin("chemical engineer", "teacher")) \
        .show()
    '''
    +---+---------+----------+--------+-----------+-------------+
    |age|firstName|   jobType|lastName|weightInLbs|     fullName|
    +---+---------+----------+--------+-----------+-------------+
    | 32|Sidhartha|Programmer|     Ray|        0.0|Sidhartha Ray|
    +---+---------+----------+--------+-----------+-------------+
    '''

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/dsl/more_functions.py
# spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.4 more_functions.py
