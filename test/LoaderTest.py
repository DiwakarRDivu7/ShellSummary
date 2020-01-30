"""

"""
import unittest
from manager.processController import selectVendor
from conf.YamlConf import YamlConf
import yaml
from pyspark.sql import SparkSession
from connectors.TargetConnector import *
from sparktestingbase.sqltestcase import SQLTestCase


class Test(SQLTestCase):
    loader = YamlConf()
    config = loader.setYamlConfig("/Users/diwr/PycharmProjects/ShellShipping/conf/shell_shipping_config.yaml")
    sprk = None

    def test_0_setYamlConfig(self):
        with open("/Users/diwr/PycharmProjects/ShellShipping/conf/shell_shipping_config.yaml") as file:
            configYaml = yaml.load(file, Loader=yaml.FullLoader)

        self.assertEqual(self.config, configYaml)


    def test_1_selectVendor(self):
        strg2 = selectVendor("ENTERPRISE MARINE SERVICES, LLC")
        self.assertEqual(strg2, strg2)

    # def test_2_getVendorsList(self):
    #     # with open("/Users/diwr/PycharmProjects/ShellShipping/conf/shell_shipping_config.yaml") as file:
    #     #     config = yaml.load(file, Loader=yaml.FullLoader)
    #
    #     lis = ["KIRBY"]
    #     lis1 = self.manager.getVendorsList(self.config, vendor="kirby")
    #     self.assertEqual(lis, lis1)
    #
    #     lis2 = ["Kirby", "Blessey"]
    #     lis3 = self.manager.getVendorsList(self.config, "all")
    #     self.assertEqual(lis2, lis3)
    #
    #     lis4 = ["KIRBY", "BLESSEY"]
    #     lis5 = self.manager.getVendorsList(self.config, "Kirby_Blessey")
    #     self.assertEqual(lis4, lis5)

    sess = singleSession()

    def test_3_createSparkSession(self):
        self.sprk = SparkSession.builder.master('local[*]').appName("test_app").getOrCreate()
        spark = self.sess.createSparkSession(self.config)
        self.assertEqual(self.sprk, spark)

        spark1 = self.sess.getSparkSession(self.config)
        self.assertEqual(self.sprk, spark1)

    def test_4_createDatasetFromCSVFile(self):
        path = self.config["Validation"]["sourcePath"]
        csv = self.sess.getSparkSession(self.config).read.option("inferschema", "false").option("header", "true").csv(path)

        readCsv = createDatasetFromCSVFile(self.config, path)

        self.assertDataFrameEqual(csv, readCsv)


    def test_5_createDatasetFromAbbyXMLFile(self):
        path = '/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/TwoMonths/Vendors/50500.xml'
        jar_path = "/Users/diwr/Desktop/DDL/jars/spark-xml_2.11-0.5.0.jar"
        test_path = "/Users/diwr/Desktop/DDL/jars/spark-fast-tests-0.17.1-s_2.11.jar"

        xml = SparkSession.builder.master("local").appName("appName") \
            .config("spark.jars", jar_path) \
            .config("spark.executor.extraClassPath", jar_path) \
            .config("spark.executor.extraLibrary", jar_path) \
            .config("spark.driver.extraClassPath", jar_path) \
            .getOrCreate().read.format("com.databricks.spark.xml") \
            .option("rowtag", "_ShellInvoices:_ShellInvoices") \
            .load(path)

        readXML = createDatasetFromAbbyXmlFile(self.config, path)
        # readXML.show()

        actual = [list(row) for row in xml.collect()]
        expected = [list(row) for row in readXML.collect()]

        self.assertEqual(actual, expected)


if __name__ == '__main__':
    # begin the unittest.main()
    unittest.main()
