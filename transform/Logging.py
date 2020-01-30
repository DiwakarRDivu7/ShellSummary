"""


"""
from conf.session import singleSession
from pyspark.sql.functions import arrays_zip, col, explode, concat, lit
from connectors.TargetConnector import writeIntoServingLayer


def logVendorDetails(config, overAllXMLCnt, vendorList, vendorInitialCnt, vendorProcessedCnt, StartTime, EndTime):
    RunID = singleSession().getSparkSession(config).sparkContext.applicationId

    masterURL = config["SparkSubmit"]["master"]
    xmlPath = config['InvoicePath']['abbyXmlPath']
    selectedColumns = config['Logging']['selectedColumns']
    summaryColumns = config['Logging']['summaryColumns']
    detailsColumns = config['Logging']['detailsColumns']
    loggingPath = config['Logging']['loggingPath']

    overAllXMLProcessedCnt = 0
    for cnt in vendorProcessedCnt:
        overAllXMLProcessedCnt += cnt

    row = [RunID, xmlPath, masterURL, overAllXMLCnt, overAllXMLProcessedCnt, StartTime, EndTime, vendorList,
           vendorInitialCnt, vendorProcessedCnt]

    dataSet = singleSession().getSparkSession(config).createDataFrame([row], selectedColumns)

    summary = dataSet.select(*summaryColumns)

    # details part
    details = dataSet.withColumn("Message",
                                 arrays_zip(col("vendorList"), col("vendorInitialCnt"), col("vendorProcessedCnt")))

    details = details.select(*detailsColumns).withColumn("Message", explode(col("Message")))\
        .withColumn("Message", concat(col("Message.vendorList"), lit(" vendor had "),
                                   col("Message.vendorInitialCnt"), lit(" XMLs and generated ") ,
                                   col("Message.vendorProcessedCnt"), lit(" records")))

    # summary.show(truncate=False)
    # details.show(truncate=False)

    # csv
    writeIntoServingLayer(summary, loggingPath + "summary", mode="Append")
    writeIntoServingLayer(details, loggingPath + "details", mode="Append")

    logSummaryOfRun = config['LoggingTale']['logSummaryOfRun']
    logDetailsOfRun = config['LoggingTale']['logDetailsOfRun']

    # Hive
    # writeIntoHiveServingLayer(summary, logSummaryOfRun)
    # writeIntoHiveServingLayer(details, logDetailsOfRun)

    return None
