"""


"""
from connectors.TargetConnector import *

def mergeFiles():

    filePath = "/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/Output/SummaryInvoice_Temp"

    filePath1 = "/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/Output/SummaryInvoice_Vendor"

    DS = createDatasetFromCSVFile(filePath)

    writeIntoServingLayer(DS,filePath1,"Overwrite")

    return None