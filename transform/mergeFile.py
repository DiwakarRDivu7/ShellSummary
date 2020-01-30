"""


"""
from connectors.TargetConnector import createDatasetFromCSVFile, writeIntoServingLayer
from pyspark.sql.functions import col, collect_set
import datetime
import os
import shutil


def mergeFiles(config, vendorDS):
    servPath = config['InvoicePath']['servingPath']
    tempPath = config['InvoicePath']['servingTempPath']
    oldServPath = config['InvoicePath']['oldServingPath']

    if os.path.exists(oldServPath):
        # list of invoice id
        filterID = vendorDS.select(collect_set(col("_InvoiceNumber"))).first()[0]

        # read the temp file
        tempDS = createDatasetFromCSVFile(config, tempPath)

        # read the older serving file
        oldServDS = createDatasetFromCSVFile(config, oldServPath)

        # get all the invoice except the invoice need to update
        oldServDS = oldServDS.filter(~(col("InvoiceID").isin(*filterID)))

        DS = oldServDS.union(tempDS)
    else:
        DS = createDatasetFromCSVFile(config, tempPath)

    # writing into serving csv
    writeIntoServingLayer(DS, servPath + "_" + str(datetime.date.today()), "Overwrite")

    # deleting temporary path
    shutil.rmtree(tempPath)
    # if os.path.exists(oldServPath):
    #     shutil.rmtree(oldServPath)
    return None
