"""

"""
from manager.ADataLoader import ADataLoader
from pyspark.sql.functions import regexp_replace, split, explode, udf
from pyspark.sql.types import DoubleType
from utils.DatasetUtils import *


class TransformKirbySummary(ADataLoader):
    def populateTable(self, config, id):

        print("inside kirby")
        import sys
        sys.exit(5)

        # requried file paths
        abbyXmlPath = config['InvoicePath']['abbyXmlPath']
        servingTempPath = config['InvoicePath']['servingTempPath']

        # create an object of the class
        kirbyObj = TransformKirbySummary()

        # requried columns to process
        colsRequired = config['Columns']['columnsRequired']

        # reading abby input files for kirby
        kirbyDS = createDatasetFromAbbyXmlFile(abbyXmlPath, id).select(*colsRequired)\
            # .filter(col("_Name").startswith("KIRBY"))

        # transform_old for lineitems
        kirbyDS = kirbyObj.transfLineitems(kirbyDS)

        # transform_old for origin and destination
        kirbyDS = kirbyObj.transfLocation(kirbyDS)

        # get book info
        kirbyDS = bookInfo(kirbyDS)

        # get bill type
        kirbyDS = billTypeInfo(kirbyDS)

        # get record status
        kirbyDS = statusOfRec(kirbyDS)

        # selected columns from a DataSet
        selected_Columns = config["Columns"]["selectedColumns"]

        # choosing required columns
        kirbyDS = kirbyDS.select(*selected_Columns)

        # columns to rename
        summary_Columns = config["Columns"]["summaryColumns"]

        # renaming the columns
        kirbyDS = kirbyDS.toDF(*summary_Columns)

        kirbyDS.show(truncate= False)
        import sys
        sys.exit(5)

        # writing into servingLayer
        writeIntoServingLayer(config, kirbyDS, servingTempPath, id)
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

        return invoiceDS
