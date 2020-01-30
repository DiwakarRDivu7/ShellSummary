"""
TargetConnector.py
~~~~~~~~

comments
"""
import datetime
from conf.session import singleSession
from pyspark.sql.functions import regexp_replace, col, collect_set, concat_ws
from pyspark.sql import DataFrame

rowTag = "_ShellInvoices:_ShellInvoices"

def createDatasetFromAbbyXmlFile(config, path):
    return singleSession().getSparkSession(config).read.format("com.databricks.spark.xml") \
            .option("rowtag", rowTag) \
            .load(path)


def createDatasetFromCSVFile(config: "", path, inferschema="false"):
    return singleSession().getSparkSession(config).read\
        .option("inferschema", inferschema).option("header", "true")\
        .csv(path)


def writeIntoServingLayer(dataSet, path, mode="Append"):

    dataSet.coalesce(1).write.mode(mode).option("header", "true").csv(path)

    return None

def writeIntoParquetServingLayer(dataSet: DataFrame, path, mode="Append"):

    dataSet.coalesce(1).write.mode(mode).option("header", "true").parquet(path)

    return None

def writeIntoHiveServingLayer(dataSet: DataFrame, tableName, mode="Append"):

    dataSet.write.mode(mode).saveAsTable(tableName)

    # dataSet.write.insertInto(tableName, mode)
    return None

# def getTranfLineitems(ds, separator=" "):
#     invoiceDS = ds
#
#     invoiceDS = invoiceDS.withColumn("TypeOfService", col("_LineItems._Description")) \
#             .withColumn("ServiceAmount", col("_LineItems._TotalPriceNetto"))
#
#     if invoiceDS.schema["TypeOfService"].dataType == ArrayType(StringType()):
#         invoiceDS = invoiceDS.withColumn("TypeOfService", concat_ws(",", col("TypeOfService"))) \
#                 .withColumn("ServiceAmount", concat_ws(",", col("ServiceAmount")))
#
#     invoiceDS = invoiceDS.withColumn("TypeOfService", regexp_replace(col("TypeOfService"), "\n", separator))
#
#     return invoiceDS

# def updateFile(config, dataSet):
#     path = config["Update"]["lastServingPath"]
#
#     filterID = dataSet.select(collect_set(col("InvoiceID"))).first()[0]
#
#     completeDS = createDatasetFromCSVFile(path).filter(~(col("InvoiceID").isin(*filterID)))
#
#     completeDS = completeDS.union(dataSet)
#
#     updatedPath = config["Update"]["updatedPath"]
#
#     completeDS.show(truncate=False)
#
#     completeDS.coalesce(1).write.mode("Overwrite").option("header", "true").csv(updatedPath + "_" + str(datetime.date.today()) )
#
#     return None



