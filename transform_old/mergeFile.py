"""


"""
from connectors.TargetConnector import *
from utils.DatasetUtils import currentDate


def mergeFiles(config):
    servingPath = config['InvoicePath']['servingPath']
    servingTempPath = config['InvoicePath']['servingTempPath']

    DS = createDatasetFromCSVFile(servingTempPath)

    writeIntoServingLayer(DS, path= servingPath + "_" + str(datetime.date.today()) , mode="Overwrite")

    return None
