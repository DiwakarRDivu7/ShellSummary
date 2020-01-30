"""

"""
from connectors.TargetConnector import createDatasetFromCSVFile
from manager.ADataLoader import ADataLoader
from pyspark.sql.functions import col, explode_outer, arrays_zip, udf, array, regexp_replace
from pyspark.sql.types import StringType
from utils.DatasetUtils import getOriginDestination, schemaPlace
import re


class EnterpriseVendor(ADataLoader):
    def populateTable(self, config, dataSet):
        # # transform for lineitems
        ds = self.transfLineitems(dataSet, config)

        # transform for origin and destination
        ds = self.transfLocation(ds)

        # ds.drop("_LineItems", "ServiceNPrice").show(200, truncate=False)
        # import sys
        # sys.exit(5)

        return ds, ds.count()

    @staticmethod
    def transfLineitems(ds, config):
        invoiceDS = ds

        invoiceDS = invoiceDS.withColumn("TypeOfService", col("_LineItems._Description")) \
            .withColumn("ServiceAmount", col("_LineItems._TotalPriceNetto"))

        if invoiceDS.schema["TypeOfService"].dataType == StringType():
            invoiceDS = invoiceDS.withColumn("TypeOfService", array(col("_LineItems._Description"))) \
                .withColumn("ServiceAmount", array(col("_LineItems._TotalPriceNetto")))

        invoiceDS = invoiceDS.withColumn("ServiceNPrice", arrays_zip(col("TypeOfService"), col("ServiceAmount"))) \
            .withColumn("ServiceNPrice", explode_outer(col("ServiceNPrice")))

        # udf_service = udf(get_enterprise_TOS, StringType())
        # invoiceDS = invoiceDS.withColumn("TypeOfService", udf_service(CostTypeList)(col("ServiceNPrice.TypeOfService"))) \
        #         .withColumn("ServiceAmount", col("ServiceNPrice.ServiceAmount"))

        billPath = config["Master"]["billPath"]
        costTypeList = createDatasetFromCSVFile("", billPath) \
            .filter(col("VENDOR_NAME").rlike("ENTERPRISE MARINE SERVICES, LLC")) \
            .rdd.map(lambda x: x.COST_LINE_ITEM_TYPE).collect()

        def udf_service(costType):
            return udf(lambda l: get_enterprise_TOS_search(l, costType), StringType())

        invoiceDS = invoiceDS.withColumn("TypeOfService", udf_service(costTypeList)(
            (regexp_replace(col("ServiceNPrice.TypeOfService"), "\n", " ")))) \
            .withColumn("ServiceAmount", col("ServiceNPrice.ServiceAmount"))

        return invoiceDS

    @staticmethod
    def transfLocation(ds):
        invoiceDS = ds

        udf_place = udf(get_enterprise_country_city_state, schemaPlace)

        invoiceDS = invoiceDS.withColumn("Origins", udf_place(col("_Origins"))) \
            .withColumn("Destinations", udf_place(col("_Destinations")))

        invoiceDS = getOriginDestination(invoiceDS)

        return invoiceDS


def get_enterprise_country_city_state(place):
    country = city = state = ""
    arrPlace = place.split("-")

    if arrPlace is None:
        ""
    else:
        if len(arrPlace) == 2:
            country = arrPlace[0].replace("(L)", "").replace("(D)", "").strip()
            citStat = arrPlace[1].split(",")
            city = citStat[0].strip()
            state = citStat[1].strip()
        else:
            country = place

    return country, city, state


def get_enterprise_TOS(service):
    TOS = ""

    if service is None:
        ""
    elif "-" in service and "Maintenance" not in service:  # (assist - values) => general
        TOS = service.split("-")[0].strip()
    elif "\n" in service:  # Fixed Day Rate
        TOS = service.split("\n")[0].strip()
    else:
        TOS = service

    if "Fuel" in TOS:  # Fuel [AMANDA] or [87654] Fuel [2345786] or check the 'terms' to get TOS
        ser = re.sub('\[.*?\]', '^', TOS).strip()
        if ser.startswith('^'):
            TOS = ser[2:].split("^")[0].strip()
        else:
            TOS = ser.split("^")[0].strip()

    return TOS


def get_enterprise_TOS_search1(service_desc, serviceList):
    TOS = ""

    for service in serviceList:
        if service.lower() in service_desc.lower():
            TOS = service
            break

    if TOS == "":
        TOS = service_desc

    return TOS

def get_enterprise_TOS_search(service_desc, serviceList):
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
