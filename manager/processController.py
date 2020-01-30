"""


"""
from connectors.TargetConnector import createDatasetFromAbbyXmlFile
from pyspark.sql.functions import collect_set, upper, col
from transform.BlesseyVendor import BlesseyVendor
from transform.KirbyVendor import KirbyVendor
from transform.EnterpriseVendor import EnterpriseVendor
from transform.GenesisVendor import GenesisVendor
from transform.AdditionalDetails import getAdditionalInfo
from transform.updateFile import updateFiles
from transform.Logging import logVendorDetails
from datetime import datetime


def initiateProcess(config, startTime):
    # vendorInitialCntList = []
    # vendorProcessedCntList = []

    # required file paths
    abbyXmlPath = config['InvoicePath']['abbyXmlPath']

    # requried columns to process
    colsRequired = config['Columns']['columnsRequired']

    # read the entire xml file from folder
    invoiceDS = createDatasetFromAbbyXmlFile(config, abbyXmlPath).select(*colsRequired).persist()

    # get the list of invoices
    vendorList = invoiceDS.select(collect_set(upper(col("_name")))).first()[0]
    # vendorList = ["GENESIS MARINE, LLC"] #['ENTERPRISE MARINE SERVICES, LLC'] #

    # start the data loader for each vendor
    countList = startDataLoader(config, invoiceDS, vendorList)

    # # loop all the vendor
    # for vend in vendorList:
    #     ven = selectVendor(vend)
    #     print("### running " + vend + " vendor ###")
    #     oneVendorDS = invoiceDS.filter(upper(col("_name")).like("%" + vend + "%")).persist()
    #     dataSet = ven.populateTable(config, oneVendorDS)
    #     getAdditionalInfo(config, dataSet[0])
    #     vendorProcessedCntList = vendorProcessedCntList + [dataSet[1]]
    #     vendorInitialCntList = vendorInitialCntList + [oneVendorDS.count()]
    #     oneVendorDS.unpersist()

    # merge the files once after all the invoices had been transformed
    print("### updating into serving ###")
    # mergeFiles(config, invoiceDS)
    updateFiles(config, invoiceDS)
    # updateHive(config, invoiceDS)
    endTime = datetime.now()

    print('### logging the job application details ###')
    # logVendorDetails(config, invoiceDS.count(), vendorList, vendorInitialCntList,  vendorProcessedCntList,
    #                  startTime, endTime)

    logVendorDetails(config, invoiceDS.count(), vendorList, countList[0], countList[1],
                     startTime, endTime)

    invoiceDS.unpersist()


def selectVendor(vendor):
    if "KIRBY" in vendor:
        vend = KirbyVendor()
    elif "BLESSEY" in vendor:
        vend = BlesseyVendor()
    elif "ENTERPRISE" in vendor:
        vend = EnterpriseVendor()
    elif "GENESIS" in vendor:
        vend = GenesisVendor()
    else:
        raise ("### obtained vendor is not in the list ###")

    return vend

def startDataLoader(config, invoiceDS, vendorList):
    vendorInitialCntList = []
    vendorProcessedCntList = []

    # loop all the vendor
    for vend in vendorList:
        ven = selectVendor(vend)
        print("### running " + vend + " vendor ###")
        oneVendorDS = invoiceDS.filter(upper(col("_name")).like("%" + vend + "%")).persist()
        dataSet = ven.populateTable(config, oneVendorDS)
        getAdditionalInfo(config, dataSet[0])
        vendorProcessedCntList = vendorProcessedCntList + [dataSet[1]]
        vendorInitialCntList = vendorInitialCntList + [oneVendorDS.count()]
        oneVendorDS.unpersist()

    return vendorInitialCntList,  vendorProcessedCntList
