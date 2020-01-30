"""

"""
from manager.ADataLoader import ADataLoader
from pyspark.sql.functions import regexp_replace, trim, udf, explode
from utils.DatasetUtils import *
from pyspark.sql.types import *


class TransformBlesseySummary(ADataLoader):
    def populateTable(self, config, id):

        print("inside blessey")
        import sys
        sys.exit(5)

        # requried file paths
        abbyXmlPath = config['InvoicePath']['abbyXmlPath']
        servingTempPath = config['InvoicePath']['servingTempPath']

        # create an object of the class
        blesseyObj = TransformBlesseySummary()

        # requried columns to process
        colsRequired = config['Columns']['columnsRequired']

        # reading abby input files for kirby
        blesseyDS = createDatasetFromAbbyXmlFile(abbyXmlPath, id).select(*colsRequired)\
            .filter(col("_Name").startswith("Blessey"))

        # transform_old for lineitems
        blesseyDS = blesseyObj.transfLineitems(blesseyDS)

        # transform_old for lineitems
        blesseyDS = blesseyObj.transfLocation(blesseyDS)

        # get book info
        blesseyDS = bookInfo(blesseyDS)

        # get bill type
        blesseyDS = billTypeInfo(blesseyDS)

        # get record status
        blesseyDS = statusOfRec(blesseyDS)

        # selected columns from a DataSet
        selected_Columns = config["Columns"]["selectedColumns"]

        # choosing requried columns
        blesseyDS = blesseyDS.select(*selected_Columns)

        # columns to rename
        summary_Columns = config["Columns"]["summaryColumns"]

        # renaming the columns
        blesseyDS = blesseyDS.toDF(*summary_Columns)

        # writing into servingLayer
        writeIntoServingLayer(config, blesseyDS, servingTempPath, id)
        return None

    @staticmethod
    def transfLineitems(ds):
        invoiceDS = ds

        invoiceDS = invoiceDS.withColumn("TypeOfService", col("_LineItems._Description"))\
            .withColumn("ServiceAmount", col("_LineItems._TotalPriceNetto"))

        if (invoiceDS.schema["TypeOfService"].dataType == ArrayType(StringType())):
            invoiceDS = invoiceDS.withColumn("TypeOfService", concat_ws(",",col("TypeOfService")))\
                .withColumn("ServiceAmount", concat_ws(",",col("ServiceAmount")))

        invoiceDS = invoiceDS.withColumn("TypeOfService", regexp_replace(col("TypeOfService"), "\n", " ")) \
            # .withColumn("ServiceAmount", col("_LineItems._TotalPriceNetto"))

        return invoiceDS

    @staticmethod
    def transfLocation(ds):
        invoiceDS = ds

        invoiceDS = invoiceDS.withColumn("_Origins", regexp_replace(col("_Origins"), "[,-]", "~")) \
            .withColumn("_Destinations", regexp_replace(col("_Destinations"), "[,-]", "~"))

        udf_Bplace = udf(get_blessey_country_city_state, schemaPlace)

        invoiceDS = invoiceDS.withColumn("Origins", udf_Bplace(col("_Origins"))) \
            .withColumn("Destinations", udf_Bplace(col("_Destinations")))

        invoiceDS = invoiceDS.withColumn("Origin", col("Origins").getField("country")) \
            .withColumn("OriginCity", trim(col("Origins").getField("city"))) \
            .withColumn("OriginState", trim(col("Origins").getField("state"))) \
            .withColumn("Destination", col("Destinations").getField("country")) \
            .withColumn("DestinationCity", trim(col("Destinations").getField("city"))) \
            .withColumn("DestinationState", trim(col("Destinations").getField("state")))

        # # to avoid St. James this scenario
        # invoiceDS = invoiceDS.withColumn("DestinationCity", regexp_replace(col("DestinationCity"), ".", ""))

        return invoiceDS
