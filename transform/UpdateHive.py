"""


"""
from conf.session import singleSession
from connectors.TargetConnector import writeIntoHiveServingLayer
from pyspark.sql.functions import col, collect_set

# updates the both consumptionTable and stagingTable
def updateHive(config, invoiceDS):
    # table names from config
    deltaTable = config['ServingTable']['deltaTable']
    stagingTable = config['ServingTable']['stagingTable']
    consumptionTable = config['ServingTable']['consumptionTable']

    # list of invoice id which we processed in this batch
    filterID = invoiceDS.select(collect_set(col("_InvoiceNumber"))).first()[0]

    # read the delta table
    deltaDS = singleSession().getSparkSession(config).sql("select * from "+ deltaTable)

    # read from staging table and get all the invoice except the invoice need to update
    stagingTable = singleSession().getSparkSession(config).sql("select * from " + stagingTable)\
        .filter(~(col("InvoiceID").isin(*filterID)))

    # union of two dataSets
    consumptionDS = stagingTable.union(deltaDS)

    # writing into both consumptionTable and stagingTable
    writeIntoHiveServingLayer(consumptionDS, consumptionTable, "OverWrite")
    writeIntoHiveServingLayer(consumptionDS, stagingTable, "OverWrite")
    return None