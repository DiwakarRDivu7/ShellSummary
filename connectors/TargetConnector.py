"""
TargetConnector.py
~~~~~~~~

comments
"""
from conf.session import singleSession

rowTag = "_ShellInvoices:_ShellInvoices"

def createDatasetFromAbbyXmlFile(path):

    return singleSession().getSparkSession().read.format("com.databricks.spark.xml")\
    .option("rowtag", rowTag)\
    .load(path)


def createDatasetFromCSVFile(path):

    return singleSession().getSparkSession().read.option("header","true").csv(path)


def writeIntoServingLayer(dataset,path,mode= "Append"):

    dataset.coalesce(1).write.mode(mode).option("header","true").csv(path)

    return None

