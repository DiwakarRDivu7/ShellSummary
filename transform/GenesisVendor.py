"""


"""
from manager.ADataLoader import ADataLoader
from pyspark.sql.functions import split, explode_outer, lit, col, regexp_replace, arrays_zip, udf, array, trim
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from connectors.TargetConnector import createDatasetFromCSVFile
import re
import sys


class GenesisVendor(ADataLoader):
    def populateTable(self, config, dataSet: DataFrame):

         # transform for lineitems
        ds = self.transfLineitems(dataSet, config)

        # transform for origin and destination
        ds = self.transfLocation(ds)

        # ds = ds.select(*["_InvoiceNumber", "_InvoiceDate", "_ShellTripID", "_VendorTripID", "_Total",
        #   "_Terms", "_BillTo", "_Name", "Origin", "OriginCity", "OriginState",
        #   "Destination", "DestinationCity", "DestinationState", "TypeOfService", "ServiceAmount"])
        #
        # ds.show(200, truncate=False)
        # sys.exit(5)

        return ds, ds.count()

    @staticmethod
    def transfLineitems(ds, config):
        invoiceDS: DataFrame = ds

        invoiceDS = invoiceDS.withColumn("TypeOfService", col("_LineItems._Description")) \
            .withColumn("ServiceAmount", col("_LineItems._TotalPriceNetto"))

        if invoiceDS.schema["TypeOfService"].dataType == StringType():
            invoiceDS = invoiceDS.withColumn("TypeOfService", array(col("_LineItems._Description"))) \
                .withColumn("ServiceAmount", array(col("_LineItems._TotalPriceNetto")))

        invoiceDS = invoiceDS.withColumn("ServiceNRate", arrays_zip(col("TypeOfService"), col("ServiceAmount")))\
                    .withColumn("ServiceNRate", explode_outer(col("ServiceNRate")))

        # udf_service = udf(get_genesis_TOS, StringType()) # same code of line 56, remove costTypeList

        billPath = config["Master"]["billPath"]
        CostTypeList = createDatasetFromCSVFile("", billPath).filter(col("VENDOR_NAME").rlike("GENESIS MARINE, LLC"))\
            .rdd.map(lambda x: x.COST_LINE_ITEM_TYPE).collect()

        def udf_service(costType):
            return udf(lambda l: get_genesis_TOS_search(l, costType), StringType())

        invoiceDS = invoiceDS.withColumn("TypeOfService", udf_service(CostTypeList)(regexp_replace(
                                                        col("ServiceNRate.TypeOfService"), "\n", " ")))\
            .withColumn("ServiceAmount", col("ServiceNRate.ServiceAmount")) \

        return invoiceDS

    @staticmethod
    def transfLocation(ds):
        invoiceDS = ds

        invoiceDS = invoiceDS.withColumn("_Origins", split(regexp_replace(col("_Origins"), "\n", " ") , "-")) \
            .withColumn("_Destinations", split(regexp_replace(col("_Destinations"), "\n", " "), "-"))

        invoiceDS = invoiceDS.withColumn("Origin", col("_Origins").getItem(0))\
            .withColumn("OriginCity", trim(col("_Origins").getItem(1))) \
            .withColumn("OriginState", lit(""))\
            .withColumn("Destination", col("_Destinations").getItem(0))\
            .withColumn("DestinationCity", trim(col("_Destinations").getItem(1))) \
            .withColumn("DestinationState", lit(""))

        return invoiceDS

def get_genesis_TOS(service_name):
    TOS = ""

    if service_name is None:
        ""
    else:
        service = "[" + service_name
        if "EXPENSES" in service:                                   # TO ReBILL DIESEL FUEL EXPENSES
            TOS = service.replace("EXPENSES", "EXPENSES]")
        elif "EXPENSE" in service:  # TO ReBILL ASSIST FEES
            TOS = service.replace("EXPENSE", "EXPENSE]")
        elif "FEES" in service:                                     # TO ReBILL ASSIST FEES
            TOS = service.replace("FEES", "FEES]")
        elif "FEE" in service:                                      # TO INVOICE LUBE FEE
            TOS = service.replace("FEE", "FEE]")
        elif "FREIGHT" in service:                                      # TO INVOICE FREIGHT
            TOS = service.replace("FREIGHT", "FREIGHT]")
        elif "TIME CHARTER" in service:  # TO INVOICE FREIGHT
            TOS = service.replace("TIME CHARTER", "TIME CHARTER]")
        else:
            TOS = service

        if "]" in TOS:
            ser = re.match(r"[^[]*\[([^]]*)\]", TOS)                 # picks the first values of []
            TOS = ser.groups()[0]             # exception, if ser is None
            if TOS.startswith("TO INVOICE"):
                TOS = TOS.replace("TO INVOICE", "").strip()
            elif TOS.startswith("TO"):
                TOS = TOS.replace("TO", "").strip()
            elif TOS.startswith("BILLING FOR"):
                TOS = TOS.replace("BILLING FOR", "").strip()

    return TOS


def get_genesis_TOS_search(service_desc, serviceList):
    TOS = ""
    temp = []
    cnt = 0

    if service_desc is not None:
        for service in serviceList:
            if service.lower() in service_desc.lower():
                cnt += 1
                temp = temp + [service]

    if len(temp) > 0:
        TOS = max(temp, key=len)

    if TOS == "":
        TOS = service_desc

    return TOS