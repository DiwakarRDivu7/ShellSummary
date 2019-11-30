"""
TransformInvoiceSummary.py
~~~~~~~~

comments
"""
from pyspark.sql.functions import concat_ws, col, regexp_replace, split, explode, udf, when, collect_set, expr, trim, regexp_extract, lit, lower, array, array_contains
from pyspark.sql.types import StringType, DoubleType, StructField, StructType, ArrayType
from connectors.TargetConnector import *
from pyspark.sql.window import Window
import sys

def populateTable():

    abbyXmlPath = "/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/TwoMonths/KirbyBlessy"
    bookPath = "/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/MasterData/Radar Data Dump.csv"
    billPath = "/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/MasterData/BillType.csv"
    servingPath = "/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/Output/SummaryInvoice_Kirby_Blessy"

    invoiceDS = createDatasetFromAbbyXmlFile(abbyXmlPath)\
                .select("_InvoiceNumber", "_InvoiceDate", "_ShellTripID", "_VendorTripID", "_Terms", "_BillTo"
                        ,"_Name", "_Origins", "_Destinations", "_Total", "_LineItems")

    schemaPlace = StructType([
        StructField("country", StringType(), False),
        StructField("city", StringType(), False),
        StructField("state", StringType(), False)
    ])

    udf_serviceprice = udf(get_custom_service_price, StringType())
    udf_place = udf(get_country_city_state, schemaPlace)
    udf_canal_location = udf(get_canal_country_city_state, schemaPlace)

    invoiceDS = invoiceDS.withColumn("BillFrom", expr("case when _Name = 'KIRBY INLAND MARINE, LP' Then 'Kirby Inland Marine' "
                                    "when _Name = 'CANALBARGE' Then 'Canal Barge' Else _Name End"))\
        .withColumn("_BillTo", regexp_replace(col("_BillTo"), "\n", ""))

    #removing few special characters for TOS and TOSPrice
    invoiceDS = invoiceDS.withColumn("LineItemsDescription", concat_ws(",", col("_LineItems._Description"))) \
        .withColumn("TOS_desctn", regexp_replace(col("LineItemsDescription"), "\n", "><")) \
        .withColumn("TOS_desc", regexp_replace(col("TOS_desctn"), "[:;\\[\\]]", "")) \
        .withColumn("Price", col("_LineItems._TotalPriceNetto"))

    #if price is not an array of double.. uses when running in delta
    if (invoiceDS.schema["Price"].dataType != ArrayType(DoubleType())):
        invoiceDS = invoiceDS.withColumn("Price", split(col("_LineItems._TotalPriceNetto"),","))

    #split line, replacing "," by "" only when lord, discharge port and From: is available
    invoiceDS = invoiceDS.withColumn("TOS", split(when( (col("TOS_desc").contains("Load port"))
                                                       | (col("TOS_desc").contains("Discharge port"))
                                                       | (col("TOS_desc").contains("From:"))
                                                       , regexp_replace(col("TOS_desc"), ",", "")).otherwise(col("TOS_desc")), ","))

    invoiceDS = invoiceDS.withColumn("servicePrice", explode(split(udf_serviceprice(col("TOS"), col("Price")), ","))) \
        .withColumn("servicePriceFinal", split(col("servicePrice"), "<>")) \
        .withColumn("TypeOfService", col("servicePriceFinal").getItem(0))\
        .withColumn("ServiceAmount", col("servicePriceFinal").getItem(1).cast(DoubleType()))

    #contains all the canalBarage's  item description in array separated by "\n"
    invoiceDS = invoiceDS.withColumn("canalDescirption", when(col("BillFrom").startswith("Canal"),split(col("TOS_desctn"),"><"))) \
        .withColumn("canalOrigin", when(((col("TOS_desctn").contains("Load port")) | (col("TOS_desctn").contains("From:"))), udf_canal_location(col("canalDescirption"),lit("Origin")))) \
        .withColumn("canalDestination", when(((col("TOS_desctn").contains("Discharge port")) | (col("TOS_desctn").contains("To:"))),udf_canal_location(col("canalDescirption"),lit("Destination")))) \

    #transforamtion on TypeOfService and TypeOfServicePrice for canal, remove the when if there is no improvement in the performance, partitioning only for canal
    #regexp_extract is helping to remove the number at the first two character (8 lube) will be (lube)
    invoiceDS = invoiceDS.withColumn("TypeOfService", when(col("BillFrom").startswith("Canal"),col("canalDescirption").getItem(0)).otherwise(col("TypeOfService"))) \
        .withColumn("TypeOfService",  when( col("BillFrom").startswith("Canal"), regexp_extract(col("TypeOfService"), "[^0-9]+.", 0)).otherwise(col("TypeOfService"))) \
        .withColumn("TypeOfService", trim(regexp_replace(col("TypeOfService"), "><", " "))) \
        .where(~(col('TypeOfService').like("Total Amount%")))

    #replace all special characters with ~ except /
    invoiceDS = invoiceDS.withColumn("_Origins", when(col("BillFrom").startswith("Blessey"),regexp_replace(col("_Origins"),"[^A-Z a-z\\/]","~")).otherwise(col("_Origins"))) \
        .withColumn("_Destinations",  when(col("BillFrom").startswith("Blessey"), regexp_replace(col("_Destinations"),"[^A-Z a-z0-9\\/]","~")).otherwise(col("_Destinations")))

    #origin and Destination => except canal we are filling the origin and destination later canal location will be empty and it is filled by using canal Origin/Dest columns
    invoiceDS = invoiceDS.withColumn("Origins", udf_place(col("_Origins"),col("BillFrom")))\
                         .withColumn("Destinations", udf_place(col("_Destinations"),col("BillFrom")))\
                         .withColumn("Origin", when(~(col("Origins").getField("country")==""), col("Origins").getField("country")).otherwise(col("canalOrigin").getField("country")))\
                         .withColumn("OriginCity", when(~(col("Origins").getField("city")==""), col("Origins").getField("city")).otherwise(col("canalOrigin").getField("city")))\
                         .withColumn("OriginState", when( ~(col("Origins").getField("state")==""), col("Origins").getField("state")).otherwise(col("canalOrigin").getField("state")))\
                         .withColumn("Destination", when(~(col("Destinations").getField("country")==""),col("Destinations").getField("country")).otherwise(col("canalDestination").getField("country")))\
                         .withColumn("DestinationCity", when(~(col("Destinations").getField("city")==""),col("Destinations").getField("city")).otherwise(col("canalDestination").getField("city")))\
                         .withColumn("DestinationState", when(~(col("Destinations").getField("state")==""),col("Destinations").getField("state")).otherwise(col("canalDestination").getField("state")))

    #reading master CSV, collecting multiple books as set and converting it into string in order to save in csv.
    bookDS = createDatasetFromCSVFile(bookPath)\
        .withColumn("BOOKS", concat_ws(",", collect_set(col("BOOK")).over(Window.partitionBy(col("TRIP"))) ))\
        .select("TRIP","BOOKS").distinct()

    # reading billType CSV
    billDS = createDatasetFromCSVFile(billPath)

    #joining invoice DS with masterBook DS to get the Book column
    invoiceDS = invoiceDS.join(bookDS, col("_ShellTripID") == col("TRIP"),"left_outer").drop("TRIP","BOOK")

    # joining invoice DS with billing DS to get the BillType column
    invoiceDS = invoiceDS.join(billDS, (lower(invoiceDS.BillFrom) == lower(billDS.VEDNOR_NAME)) & (lower(invoiceDS.TypeOfService) == lower(billDS.COST_LINE_ITEM_TYPE)), "left_outer")\
        .drop("VEDNOR_NAME", "COST_LINE_ITEM_TYPE")

    invoiceDS = invoiceDS.withColumn("row_status", when( col("BOOKS").contains(",")
                                                         | col("Origin").rlike("~|/")
                                                         | col("Destination").rlike("~|/")
                                                         ,"Bad_Rec").otherwise("Good_Rec"))

    invoiceDS = invoiceDS.select("row_status","_InvoiceNumber", "_InvoiceDate", "_ShellTripID", "_VendorTripID", "_Total", "_Terms", "_BillTo", "BillFrom",
                     "Origin","OriginCity","OriginState","Destination","DestinationCity","DestinationState", "TypeOfService", "ServiceAmount","BOOKS","COST_TYPE")

    summary_Columns = ["row_status","InvoiceID","InvoiceDate","ShellTripID","VendorTripID", "TotalAmountDue","Terms", "BillTo","BillFrom",
                       "Origin","OriginCity","OriginState","Destination","DestinationCity","DestinationState","TypeOfService", "AmountDue","Book","BillType"]

    invoiceDS = invoiceDS.toDF(*summary_Columns)

    writeIntoServingLayer(invoiceDS,servingPath)
    # invoiceDS.show(200,truncate=False)#printSchema()#
    return None


def get_custom_service_price(serv,price):
    pair = ""
    if serv is None or price is None:
        pair
    else:
        if len(serv)==len(price):
            for i in range(0,len(serv)):
                pair = pair + serv[i]+ "<>" +str(price[i]) + ","
        else: print("length doesn't match")
    return pair[:-1]



def get_country_city_state(placee,vendor):

    country = ""
    city = ""
    state = ""
    lis =  ["", "", "", ""]
    lis1 =  ["", "", "", ""]
    lis2 = ["", "", "", ""]

    if("Kirby" in vendor):
        arrPlace = placee.split("/")

        if arrPlace is None:
            ""
        else :
            if len(arrPlace) == 2:
                country = arrPlace[0]
                city = arrPlace[1][0:-3]
                state = arrPlace[1][-2:]
            elif len(arrPlace) == 3:
                country = arrPlace[0]
                city = arrPlace[1]
                state = arrPlace[2]
            else:
                country = placee
    elif("BlesseyOld" in vendor):
        arrPlace = placee.replace("/",",").split(",")

        if arrPlace is None:
            ""
        else:
            if len(arrPlace) == 2:
                city = arrPlace[0]
                state = arrPlace[1].replace(".","").strip()
            elif len(arrPlace) == 3:
                country = arrPlace[0]
                city = arrPlace[1]
                state = arrPlace[2]
            else:
                country = placee
    elif ("Blessey" in vendor):
        place=placee
        if(place[-1] == "~"): place = place[:-1]    #after state value remove if contains "~"(before regex it was "." in the column)
        if ("/" in place):
            arr = place.split("/")          # Ergon~Marietta~ OH/Enlink~Bells Run~ OH
            arr0 = arr[0].split("~")
            if (len(arr0) < 4):
                for x in range(0, len(arr0)):
                    lis[x] = arr0[x]                #first list contains, first set upto /
                    # print(lis)
            arr1 = arr[1].split("~")
            if(len(arr1)<4):
                for y in range(0, len(arr1)):    #if(y<3):
                    lis1[y] = arr1[y]               #second list contains, second set upto /
                    # print(lis1)
            if (len(arr) == 3):
                arr2 = arr[2].split("~")
                if (len(arr2) < 4):
                    for z in range(0, len(arr2)):
                        lis2[z] = arr2[z]               #third list contains, third set
                        # print(lis2)
        elif (place == "TBN"):
            for t in range(0, 3):
                lis[t] = "TBN"
                # print(lis)
        else:
            ar = place.split("~")               #Enlink~Bells Run~ OH  = direct, which doesn't have second place
            if ar is None:
                ""
            else:
                if(len(ar)==3):
                    for x in range(0, len(ar)):
                        lis[x] = ar[x]
                        # print(lis)
                elif(len(ar)==2):
                    st = ar[1].strip() #check its state or not, triming for white space
                    if(len(st)==2):
                        lis[1]=ar[0]
                        lis[2]=st
                    else:
                        lis[0] = ar[0]
                        lis[1] = st

    #replace if city and state is empty by other list
    if ("Blessey" in vendor):
        country = lis[0]
        city = lis[1]
        state = lis[2]
        if (city == ""):
            city = lis1[1]
            state = lis1[2]

        if (state == ""):
            state = lis2[2]

        if(country =="" and city == "" and state== ""):
            country=placee

    return country,city,state

#passing entire array
def get_canal_country_city_state(desc,place):

    country = ""
    city = ""
    state = ""

    if desc is None:
        ""
    else:
        if(place == "Origin"):
            for i in range(0,len(desc)):
                if('From:' in desc[i]):
                    loc = desc[i].split(",")
                    country = desc[i+1]
                    city = loc[0].replace("From:","").strip()
                    state = loc[1].replace(".","").strip()
                elif ('Load port' in desc[i]):
                        loc = desc[i].split(",")
                        city = loc[0].replace("Load port","").strip()
                        state = loc[1].replace(".","").strip()
        else:
            for i in range(0,len(desc)):
                if('To:' in desc[i]):
                    loc = desc[i].split(",")
                    country = desc[i+1]
                    city = loc[0].replace("To:","").strip()
                    state = loc[1].replace(".","").strip()
                elif ('Discharge port' in desc[i]):
                    loc = desc[i].split(",")
                    city = loc[0].replace("Discharge port","").strip()
                    state = loc[1].replace(".","").strip()

    return country,city,state