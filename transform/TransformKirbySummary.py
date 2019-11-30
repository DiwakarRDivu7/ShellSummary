"""

"""
from manager.ADataLoader import ADataLoader
from pyspark.sql.functions import regexp_replace, split, explode, udf
from pyspark.sql.types import DoubleType
from utils.DatasetUtils import *


class TransformKirbySummary(ADataLoader):
    def populateTable(self):
        # requried file paths
        abbyXmlPath = "/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/TwoMonths/Kirby"
        servingPath = "/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/Output/SummaryInvoice_Temp"

        # create an object of the class
        kirbyObj = TransformKirbySummary()

        # requried columns to process
        colsRequried = ["_InvoiceNumber", "_InvoiceDate", "_ShellTripID", "_VendorTripID", "_Terms", "_BillTo",
                        "_Name", "_Origins", "_Destinations", "_Total", "_LineItems"]

        # reading abby input files for kirby
        kirbyDS = createDatasetFromAbbyXmlFile(abbyXmlPath).select(*colsRequried)

        # transform for lineitems
        kirbyDS = kirbyObj.transfLineitems(kirbyDS)

        # transform for origin and destination
        kirbyDS = kirbyObj.transfLocation(kirbyDS)

        # get book info
        kirbyDS = bookInfo(kirbyDS)

        # get bill type
        kirbyDS = billTypeInfo(kirbyDS)

        # get record status
        kirbyDS = statusOfRec(kirbyDS)

        # choosing requried columns
        kirbyDS = kirbyDS.select("row_status", "_InvoiceNumber", "_InvoiceDate", "_ShellTripID", "_VendorTripID",
                                 "_Total", "_Terms", "_BillTo", "_Name",
                                 "Origin", "OriginCity", "OriginState", "Destination", "DestinationCity",
                                 "DestinationState", "TypeOfService", "ServiceAmount", "BOOKS", "COST_TYPE")

        # columns to rename
        summary_Columns = ["row_status", "InvoiceID", "InvoiceDate", "ShellTripID", "VendorTripID", "TotalAmountDue",
                           "Terms", "BillTo", "BillFrom",
                           "Origin", "OriginCity", "OriginState", "Destination", "DestinationCity", "DestinationState",
                           "TypeOfService", "AmountDue", "Book", "BillType"]

        # renaming the columns
        kirbyDS = kirbyDS.toDF(*summary_Columns)

        # writing into servingLayer
        writeIntoServingLayer(kirbyDS, servingPath)
        return None

    @staticmethod
    def transfLineitems(ds):
        invoiceDS = ds

        # removing few special characters for TOS and TOSPrice
        invoiceDS = invoiceDS.withColumn("LineItemsDescription", concat_ws(",", col("_LineItems._Description"))) \
            .withColumn("TOS", split(regexp_replace(col("LineItemsDescription"), "[:;\\[\\]]", ""), ",")) \
            .withColumn("Price", col("_LineItems._TotalPriceNetto"))

        udf_serviceprice = udf(get_custom_service_price, StringType())

        invoiceDS = invoiceDS.withColumn("servicePrice",
                                         explode(split(udf_serviceprice(col("TOS"), col("Price")), ","))) \
            .withColumn("servicePriceFinal", split(col("servicePrice"), "<>")) \
            .withColumn("TypeOfService", col("servicePriceFinal").getItem(0)) \
            .withColumn("ServiceAmount", col("servicePriceFinal").getItem(1).cast(DoubleType())) \
            .where(~(col('TypeOfService').like("Total Amount%")))

        return invoiceDS

    @staticmethod
    def transfLocation(ds):
        invoiceDS = ds

        udf_place = udf(get_kirby_country_city_state, schemaPlace)

        invoiceDS = invoiceDS.withColumn("Origins", udf_place(col("_Origins"))) \
            .withColumn("Destinations", udf_place(col("_Destinations")))

        invoiceDS = invoiceDS.withColumn("Origin", col("Origins").getField("country")) \
            .withColumn("OriginCity", col("Origins").getField("city")) \
            .withColumn("OriginState", col("Origins").getField("state")) \
            .withColumn("Destination", col("Destinations").getField("country")) \
            .withColumn("DestinationCity", col("Destinations").getField("city")) \
            .withColumn("DestinationState", col("Destinations").getField("state"))

        # to avoid St. James this scenario
        invoiceDS = invoiceDS.withColumn("DestinationCity", regexp_replace(col("DestinationCity"), ".", ""))

        return invoiceDS
