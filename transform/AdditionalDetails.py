"""


"""
from pyspark.sql.functions import concat_ws, collect_set, col, lower, when, current_timestamp
from pyspark.sql.window import Window
from connectors.TargetConnector import createDatasetFromCSVFile, writeIntoServingLayer, writeIntoHiveServingLayer
from pyspark.sql import DataFrame


def getAdditionalInfo(config, ds: DataFrame):
    bookPath = config["Master"]["bookPath"]

    billPath = config["Master"]["billPath"]

    # get book info
    dataSet = bookInfo(ds, bookPath)

    # get bill type
    dataSet = billTypeInfo(dataSet, billPath)

    # get record status
    dataSet = statusOfRec(dataSet)

    # selected columns from a DataSet
    selected_Columns = config["Columns"]["selectedColumns"]

    # choosing required columns
    dataSet = dataSet.select(*selected_Columns)

    # columns to rename
    summary_Columns = config["Columns"]["summaryColumns"]

    # renaming the columns
    dataSet = dataSet.toDF(*summary_Columns)

    # required file paths
    servingTempPath = config['InvoicePath']['servingTempPath']

    deltaTable = config['ServingTable']['deltaTable']

    # dataSet.printSchema() #.filter(col("AmountDue._nil") == True)
    # dataSet.show(500, truncate=False)
    # import sys
    # sys.exit(5)

    # writing into servingLayer
    writeIntoServingLayer(dataSet, servingTempPath)
    # writeIntoHiveServingLayer(dataSet, deltaTable, "OverWrite")
    return None


def bookInfo(ds, bookPath):
    # reading master CSV, collecting multiple books as set and converting it into string in order to save in csv.
    bookDS = createDatasetFromCSVFile("", path=bookPath) \
        .withColumn("BOOKS", concat_ws(",", collect_set(col("BOOK")).over(Window.partitionBy(col("TRIP"))))) \
        .select("TRIP", "BOOKS").distinct()

    # joining invoice DS with masterBook DS to get the Book column
    DS = ds.join(bookDS, col("_ShellTripID") == col("TRIP"), "left_outer").drop("TRIP", "BOOK")

    return DS


def billTypeInfo(ds, billPath):
    DS = ds

    # reading billType CSV
    billDS = createDatasetFromCSVFile("", billPath)

    # joining invoice DS with billing DS to get the BillType column, use rename option while joining to remove grey warn
    DS = DS.join(billDS,
                 (lower(DS._Name) == lower(billDS.VENDOR_NAME)) &
                 (lower(DS.TypeOfService) == lower(billDS.COST_LINE_ITEM_TYPE)),
                 "left_outer") \
        .drop("VEDNOR_NAME", "COST_LINE_ITEM_TYPE")

    return DS


def statusOfRec(ds):
    DS = ds.withColumn("row_status", when(col("BOOKS").contains(",")
                                          | col("Origin").rlike("~|/")
                                          | col("Destination").rlike("~|/")
                                          | col("DestinationCity").contains(".")
                                          | col("OriginCity").contains(".")
                                          | col("COST_TYPE").isNull()
                                          | (col("DestinationState") == "")
                                          | (col("OriginState") == "")
                                          , "Bad_Rec").otherwise("Good_Rec")) \
        .withColumn("ProcessingTime", current_timestamp())

    DS = DS.withColumn("status_reason", when(col("BOOKS").contains(","), "Multiple Books")
                       .when(col("Origin").rlike("~|/") | col("Destination").rlike("~|/")
                             | col("DestinationCity").contains(".") | col("OriginCity").contains(".")
                             | (col("DestinationState") == "") | (col("OriginState") == ""),
                             "Unexpected special characters or empty state/city in Origins/Destinations")
                       .when(col("COST_TYPE").isNull(), "No Bill Type").otherwise("-")) \

    return DS
