"""
session.py
~~~~~~~~

comments
"""
from pyspark.sql import SparkSession

class singleSession:
    spark:SparkSession = None

    def createSparkSession(self):

        if(singleSession.spark==None):
            jar_path = "/Users/diwr/Desktop/DDL/jars/spark-xml_2.11-0.5.0.jar"

            # get Spark session factory
            singleSession.spark = SparkSession.builder.master('local[*]').appName("test_app") \
                .config("spark.jars", jar_path) \
                .config("spark.executor.extraClassPath", jar_path) \
                .config("spark.executor.extraLibrary", jar_path) \
                .config("spark.driver.extraClassPath", jar_path) \
                .getOrCreate()
                # .config("spark.sql.shuffle.partitions", "5") \
        else:
            Exception("call getSession() function to get the spark session")

        singleSession.spark.sparkContext.setLogLevel("error")
        return singleSession.spark

    def getSparkSession(self):

        if(singleSession.spark == None):
            singleSession.spark = singleSession().createSparkSession()

        return singleSession.spark
