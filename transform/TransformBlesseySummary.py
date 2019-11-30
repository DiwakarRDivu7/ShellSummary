"""

"""
from manager.ADataLoader import ADataLoader
from pyspark.sql.functions import regexp_replace, trim, udf
from utils.DatasetUtils import *


class TransformBlesseySummary(ADataLoader):
    def populateTable(self):

        # requried file paths
        abbyXmlPath = "/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/TwoMonths/Blessey"
        servingPath = "/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/Output/SummaryInvoice_Temp"

        # create an object of the class
        blesseyObj = TransformBlesseySummary()

        # requried columns to process
        colsRequried = ["_InvoiceNumber", "_InvoiceDate", "_ShellTripID", "_VendorTripID", "_Terms", "_BillTo",
                        "_Name", "_Origins", "_Destinations", "_Total", "_LineItems"]

        # reading abby input files for kirby
        blesseyDS = createDatasetFromAbbyXmlFile(abbyXmlPath).select(*colsRequried)

        # transform for lineitems
        blesseyDS = blesseyObj.transfLineitems(blesseyDS)

        # transform for lineitems
        blesseyDS = blesseyObj.transfLocation(blesseyDS)

        # get book info
        blesseyDS = bookInfo(blesseyDS)

        # get bill type
        blesseyDS = billTypeInfo(blesseyDS)

        # get record status
        blesseyDS = statusOfRec(blesseyDS)

        # choosing requried columns
        blesseyDS = blesseyDS.select("row_status", "_InvoiceNumber", "_InvoiceDate", "_ShellTripID", "_VendorTripID",
                                     "_Total", "_Terms", "_BillTo", "_Name",
                                     "Origin", "OriginCity", "OriginState", "Destination", "DestinationCity",
                                     "DestinationState", "TypeOfService", "ServiceAmount", "BOOKS", "COST_TYPE")

        # columns to rename
        summary_Columns = ["row_status", "InvoiceID", "InvoiceDate", "ShellTripID", "VendorTripID", "TotalAmountDue",
                           "Terms", "BillTo", "BillFrom",
                           "Origin", "OriginCity", "OriginState", "Destination", "DestinationCity", "DestinationState",
                           "TypeOfService", "AmountDue", "Book", "BillType"]

        # renaming the columns
        blesseyDS = blesseyDS.toDF(*summary_Columns)

        # writing into servingLayer
        writeIntoServingLayer(blesseyDS, servingPath)
        return None

    @staticmethod
    def transfLineitems(ds):
        invoiceDS = ds

        invoiceDS = invoiceDS.withColumn("TypeOfService", regexp_replace(col("_LineItems._Description"), "\n", " ")) \
            .withColumn("ServiceAmount", col("_LineItems._TotalPriceNetto"))

        return invoiceDS

    @staticmethod
    def transfLocation(ds):
        invoiceDS = ds

        invoiceDS = invoiceDS.withColumn("_Origins", regexp_replace(col("_Origins"), "[,-]", "~")) \
            .withColumn("_Destinations", regexp_replace(col("_Destinations"), "[,-]", "~"))

        udf_place = udf(get_blessey_country_city_state, schemaPlace)

        invoiceDS = invoiceDS.withColumn("Origins", udf_place(col("_Origins"))) \
            .withColumn("Destinations", udf_place(col("_Destinations")))

        invoiceDS = invoiceDS.withColumn("Origin", col("Origins").getField("country")) \
            .withColumn("OriginCity", trim(col("Origins").getField("city"))) \
            .withColumn("OriginState", trim(col("Origins").getField("state"))) \
            .withColumn("Destination", col("Destinations").getField("country")) \
            .withColumn("DestinationCity", trim(col("Destinations").getField("city"))) \
            .withColumn("DestinationState", trim(col("Destinations").getField("state")))

        return invoiceDS
