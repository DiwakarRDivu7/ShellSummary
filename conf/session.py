"""
session.py
~~~~~~~~

comments
"""
from pyspark.sql import SparkSession


class singleSession:
    spark: SparkSession = None

    @staticmethod
    def createSparkSession(config):

        if singleSession.spark is None:
            jar_path = config["ExternalJars"]["sparkXML"] #"/Users/diwr/Desktop/DDL/jars/spark-xml_2.11-0.5.0.jar"  #
            master = config["SparkSubmit"]["master"]
            appName = config["SparkSubmit"]["appName"]
            logLevel = config["SparkSubmit"]["logLevel"]

            # get Spark session factory
            singleSession.spark = SparkSession.builder.master(master).appName(appName) \
                .config('spark.sql.codegen.wholeStage', 'false')\
                .config("spark.jars", jar_path) \
                .config("spark.executor.extraClassPath", jar_path) \
                .config("spark.executor.extraLibrary", jar_path) \
                .config("spark.driver.extraClassPath", jar_path) \
                .enableHiveSupport()\
                .getOrCreate()
        else:
            Exception("call getSession() function to get the spark session")

        singleSession.spark.sparkContext.setLogLevel(logLevel)
        return singleSession.spark

    @staticmethod
    def getSparkSession(config= None):

        if singleSession.spark is None:
            singleSession.spark = singleSession().createSparkSession(config)

        return singleSession.spark
