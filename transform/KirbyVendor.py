"""

"""
from manager.ADataLoader import ADataLoader
from pyspark.sql.functions import split, explode_outer, udf, concat_ws, col, regexp_replace, arrays_zip, when
from pyspark.sql.types import DoubleType, StringType
from utils.DatasetUtils import getOriginDestination, get_kirby_country_city_state, get_custom_service_price, schemaPlace
from pyspark.sql import DataFrame


class KirbyVendor(ADataLoader):
    def populateTable(self, config, dataSet: DataFrame):

        # transform for lineitems
        ds = self.transfLineitems(dataSet)

        # transform for origin and destination
        ds = self.transfLocation(ds)

        return ds, ds.count()

    @staticmethod
    def transfLineitems_old(ds):
        invoiceDS = ds

        # always an array, if format changes... no amount due in line items.. then check for an array type(_Description)

        # removing few special characters for TOS and TOSPrice
        invoiceDS = invoiceDS.withColumn("LineItemsDescription", concat_ws(",", col("_LineItems._Description"))) \
            .withColumn("TOS", split(regexp_replace(col("LineItemsDescription"), "[:;\\[\\]]", ""), ",")) \
            .withColumn("Price", col("_LineItems._TotalPriceNetto"))

        udf_serviceprice = udf(get_custom_service_price, StringType())

        invoiceDS = invoiceDS.withColumn("servicePrice",
                                         explode_outer(split(udf_serviceprice(col("TOS"), col("Price")), ","))) \
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

        invoiceDS = getOriginDestination(invoiceDS)

        return invoiceDS

    @staticmethod
    def transfLineitems(ds):
        invoiceDS: DataFrame = ds

        invoiceDS = invoiceDS.withColumn("TypeOfService", col("_LineItems._Description")) \
            .withColumn("ServiceAmount", col("_LineItems._TotalPriceNetto"))

        if invoiceDS.schema["TypeOfService"].dataType == StringType():
            invoiceDS = invoiceDS.withColumn("TypeOfService", split(col("TypeOfService"), ",")) \
                .withColumn("ServiceAmount", split(col("ServiceAmount"), ","))

        # invoiceDS.printSchema()
        # import sys
        # sys.exit(3)

        invoiceDS = invoiceDS.withColumn("ServiceNPrice", arrays_zip(col("TypeOfService"), col("ServiceAmount"))) \
            .withColumn("ServiceNPrice", explode_outer(col("ServiceNPrice")))

        invoiceDS = invoiceDS.withColumn("TypeOfService", when(col("ServiceNPrice.TypeOfService").isNotNull(),
                                         regexp_replace(col("ServiceNPrice.TypeOfService"), "[:;]", "")).otherwise(""))\
            .withColumn("ServiceAmount", col("ServiceNPrice.ServiceAmount")) \
            .filter(~(col('TypeOfService').like("Total Amount%")))

        return invoiceDS
