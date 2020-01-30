"""


"""


import unittest
from pyspark.sql import SparkSession
from connectors.TargetConnector import createDatasetFromAbbyXmlFile
import yaml

class Test(unittest.TestCase):
    with open("/Users/diwr/PycharmProjects/ShellShipping/conf/shell_shipping_config.yaml") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    # xx = createDatasetFromAbbyXmlFile(config, '/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/TwoMonths/Vendors/50500.xml')

    def test_0_createDatasetFromAbbyXMLFile(self):

        path = '/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/TwoMonths/Vendors/50500.xml'
        jar_path = "/Users/diwr/Desktop/DDL/jars/spark-xml_2.11-0.5.0.jar"

        xml = SparkSession.builder.master("local").appName("appName") \
            .config("spark.jars", jar_path) \
            .config("spark.executor.extraClassPath", jar_path) \
            .config("spark.executor.extraLibrary", jar_path) \
            .config("spark.driver.extraClassPath", jar_path) \
            .getOrCreate().read.format("com.databricks.spark.xml") \
            .option("rowtag", "_ShellInvoices:_ShellInvoices") \
            .load(path)

        readXML = createDatasetFromAbbyXmlFile(self.config, path)

        actual = [list(row) for row in xml.collect()]
        expected = [list(row) for row in readXML.collect()]

        self.assertEqual(actual, expected)