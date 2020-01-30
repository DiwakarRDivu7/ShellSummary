"""

"""
from manager.ADataLoader import ADataLoader
from pyspark.sql.functions import regexp_replace, udf, concat_ws
from utils.DatasetUtils import get_blessey_country_city_state, schemaPlace, col, getOriginDestination
from pyspark.sql.types import ArrayType, StringType


class BlesseyVendor(ADataLoader):
    def populateTable(self, config, dataSet):

        # transform for blessey lineitems
        ds = self.transfLineitems(dataSet)

        # transform for blessey location
        ds = self.transfLocation(ds)

        return ds, ds.count()

    @staticmethod
    def transfLineitems(ds):
        invoiceDS = ds

        invoiceDS = invoiceDS.withColumn("TypeOfService", col("_LineItems._Description")) \
            .withColumn("ServiceAmount", col("_LineItems._TotalPriceNetto"))

        if invoiceDS.schema["TypeOfService"].dataType == ArrayType(StringType()):
            invoiceDS = invoiceDS.withColumn("TypeOfService", concat_ws(",", col("TypeOfService"))) \
                .withColumn("ServiceAmount", concat_ws(",", col("ServiceAmount")))

        invoiceDS = invoiceDS.withColumn("TypeOfService", regexp_replace(col("TypeOfService"), "\n", " "))

        return invoiceDS

    @staticmethod
    def transfLocation(ds):
        invoiceDS = ds

        invoiceDS = invoiceDS.withColumn("_Origins", regexp_replace(col("_Origins"), "[,-]", "~")) \
            .withColumn("_Destinations", regexp_replace(col("_Destinations"), "[,-]", "~"))

        udf_Bplace = udf(get_blessey_country_city_state, schemaPlace)

        invoiceDS = invoiceDS.withColumn("Origins", udf_Bplace(col("_Origins"))) \
            .withColumn("Destinations", udf_Bplace(col("_Destinations")))

        invoiceDS = getOriginDestination(invoiceDS)

        # # to avoid St. James this scenario
        # invoiceDS = invoiceDS.withColumn("DestinationCity", regexp_replace(col("DestinationCity"), ".", ""))

        return invoiceDS
