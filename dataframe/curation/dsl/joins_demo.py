from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from Role import Role
from Employee import Employee

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    emp_df = spark.createDataFrame([
        Employee(1, "Sidhartha", "Ray"),
        Employee(2, "Pratik", "Solanki"),
        Employee(3, "Ashok", "Pradhan"),
        Employee(4, "Rohit", "Bhangur"),
        Employee(5, "Kranti", "Meshram"),
        Employee(7, "Ravi", "Kiran")
    ])

    role_df = spark.createDataFrame([
        Role(1, "Architect"),
        Role(2, "Programmer"),
        Role(3, "Analyst"),
        Role(4, "Programmer"),
        Role(5, "Architect"),
        Role(6, "CEO")
    ])

    # employeeDf.join(empRoleDf, "id" === "id").show(false)   #Ambiguous column name "id"
    emp_df.join(role_df, emp_df.id == role_df.id).show(5, False)
    '''
    +---------+---+--------+---+----------+
    |firstName|id |lastName|id |jobRole   |
    +---------+---+--------+---+----------+
    |Kranti   |5  |Meshram |5  |Architect |
    |Sidhartha|1  |Ray     |1  |Architect |
    |Ashok    |3  |Pradhan |3  |Analyst   |
    |Pratik   |2  |Solanki |2  |Programmer|
    |Rohit    |4  |Bhangur |4  |Programmer|
    +---------+---+--------+---+----------+
    '''
    emp_df.join(broadcast(role_df), emp_df["id"] == role_df["id"], "inner").show(5, False)

    # Join Types: "left_outer"/"left", "full_outer"/"full"/"outer"
    emp_df.join(role_df, [emp_df["id"] == role_df["id"]], "inner").show()
    emp_df.join(role_df, [emp_df["id"] == role_df["id"]], "right_outer").show()
    emp_df.join(role_df, [emp_df["id"] == role_df["id"]], "left_anti").show()
    emp_df.join(role_df, [emp_df["id"] == role_df["id"]], "full").show()

    # cross join
    emp_df.join(role_df, [emp_df["id"] == role_df["id"]], "cross").show()
# map-side join, sort-merge join, SQL version of above
# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/dsl/joins_demo.py
# spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.4 dataframe\curation\dsl\joins_demo.py

'''
+---------+---+--------+---+----------+
|firstName|id |lastName|id |jobRole   |
+---------+---+--------+---+----------+
|Kranti   |5  |Meshram |5  |Architect |
|Sidhartha|1  |Ray     |1  |Architect |
|Ashok    |3  |Pradhan |3  |Analyst   |
|Pratik   |2  |Solanki |2  |Programmer|
|Rohit    |4  |Bhangur |4  |Programmer|
+---------+---+--------+---+----------+

+---------+---+--------+---+----------+
|firstName|id |lastName|id |jobRole   |
+---------+---+--------+---+----------+
|Sidhartha|1  |Ray     |1  |Architect |
|Pratik   |2  |Solanki |2  |Programmer|
|Ashok    |3  |Pradhan |3  |Analyst   |
|Rohit    |4  |Bhangur |4  |Programmer|
|Kranti   |5  |Meshram |5  |Architect |
+---------+---+--------+---+----------+

+---------+---+--------+---+----------+
|firstName| id|lastName| id|   jobRole|
+---------+---+--------+---+----------+
|   Kranti|  5| Meshram|  5| Architect|
|Sidhartha|  1|     Ray|  1| Architect|
|    Ashok|  3| Pradhan|  3|   Analyst|
|   Pratik|  2| Solanki|  2|Programmer|
|    Rohit|  4| Bhangur|  4|Programmer|
+---------+---+--------+---+----------+

+---------+----+--------+---+----------+
|firstName|  id|lastName| id|   jobRole|
+---------+----+--------+---+----------+
|     null|null|    null|  6|       CEO|
|   Kranti|   5| Meshram|  5| Architect|
|Sidhartha|   1|     Ray|  1| Architect|
|    Ashok|   3| Pradhan|  3|   Analyst|
|   Pratik|   2| Solanki|  2|Programmer|
|    Rohit|   4| Bhangur|  4|Programmer|
+---------+----+--------+---+----------+

+---------+---+--------+
|firstName| id|lastName|
+---------+---+--------+
|     Ravi|  7|   Kiran|
+---------+---+--------+

+---------+----+--------+----+----------+
|firstName|  id|lastName|  id|   jobRole|
+---------+----+--------+----+----------+
|     Ravi|   7|   Kiran|null|      null|
|     null|null|    null|   6|       CEO|
|   Kranti|   5| Meshram|   5| Architect|
|Sidhartha|   1|     Ray|   1| Architect|
|    Ashok|   3| Pradhan|   3|   Analyst|
|   Pratik|   2| Solanki|   2|Programmer|
|    Rohit|   4| Bhangur|   4|Programmer|
+---------+----+--------+----+----------+

+---------+---+--------+---+----------+
|firstName| id|lastName| id|   jobRole|
+---------+---+--------+---+----------+
|   Kranti|  5| Meshram|  5| Architect|
|Sidhartha|  1|     Ray|  1| Architect|
|    Ashok|  3| Pradhan|  3|   Analyst|
|   Pratik|  2| Solanki|  2|Programmer|
|    Rohit|  4| Bhangur|  4|Programmer|
+---------+---+--------+---+----------+
'''