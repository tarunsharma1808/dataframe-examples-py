from pyspark.sql import SparkSession, Row
from distutils.util import strtobool
import os.path
import yaml
import sys

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("RDD examples") \
        .master('local[*]') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_file_path = os.path.abspath(current_dir + "/../" + "application.yml")

    with open(app_config_file_path) as conf:
        doc = yaml.load(conf, Loader=yaml.FullLoader)

    # Read access key and secret key from cmd line argument
    s3_access_key = doc["s3_conf"]["access_key"]
    s3_secret_access_key = doc["s3_conf"]["secret_access_key"]
    if len(sys.argv) > 1 and sys.argv[1] is not None and sys.argv[2] is not None:
        s3_access_key = sys.argv[1]
        s3_secret_access_key = sys.argv[2]

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", s3_access_key)
    hadoop_conf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    demographics_rdd = spark.sparkContext.textFile("s3a://" + doc["s3_conf"]["s3_bucket"] + "/demographic.csv")
    finances_rdd = spark.sparkContext.textFile("s3a://" + doc["s3_conf"]["s3_bucket"] + "/finances.csv")

    demographics_pair_rdd = demographics_rdd \
        .map(lambda line: line.split(",")) \
        .map(lambda lst: (int(lst[0]), int(lst[1]), strtobool(lst[2]), lst[3], lst[4], strtobool(lst[5]), strtobool(lst[6]), int(lst[7])))

    finances_pair_rdd = finances_rdd \
        .map(lambda line: line.split(",")) \
        .map(lambda lst: (int(lst[0]), strtobool(lst[1]), strtobool(lst[2]), strtobool(lst[3]), int(lst[4])))

    join_pair_rdd = demographics_pair_rdd.cartesian(finances_pair_rdd)\
        .filter(lambda rec: rec[0][0] == rec[1][0])  \
        .filter(lambda rec: (rec[0][3] == "Switzerland") and (rec[1][1]) and (rec[1][2]))

    join_pair_rdd.foreach(print)

