"""
compare the serving layer data with the ecap data with attribute level and also provide the
status of the row with the description.
"""

from connectors.TargetConnector import createDatasetFromCSVFile, writeIntoServingLayer, writeIntoParquetServingLayer
from pyspark.sql.functions import col, sum, when, round, regexp_replace, \
    lower, concat, lit, to_timestamp, unix_timestamp, collect_list, concat_ws, date_format, udf, split
from pyspark.sql.types import DoubleType, TimestampType
from pyspark.sql.window import Window
from utils.DatasetUtils import getYamlConfig
from datetime import datetime
from pyspark.sql.types import StringType
import sys


def ecapValidation():
    # noting the start time
    startTime = datetime.now().time().strftime('%H:%M:%S')

    confPath = ""
    fromDate = ''
    toDate = ''
    argLen = len(sys.argv)

    # Check if received correct number of arguments.
    if argLen == 1:
        raise ("### configuration file path must be specified as argument ###")
    elif argLen == 2:
        confPath = sys.argv[1]
    elif argLen == 3:  # remove elif if fromDate toDate are picking from yaml
        raise ("### argument length should be 3, mention fromDate toDate in MM-dd-yyyy format ###")
    elif argLen == 4:
        confPath = sys.argv[1]
        fromDate = sys.argv[2]
        toDate = sys.argv[3]

    # get the data from configuration file
    config = getYamlConfig(confPath)

    # list of primary keyColumn
    primary_key_column = config['Ecap']['primaryKeyColumns']

    # get the serving data to compare
    servingDS = getServingData(config, primary_key_column, fromDate, toDate)

    # get the ecap data to compare
    ecapDS = getEcapData(config, primary_key_column, fromDate, toDate)

    # compare the serving and ecap data
    comapredDS = compareServingEcapDS(config, servingDS, ecapDS, primary_key_column)

    # get the row status and the description for compare dataset
    validatedDS = rowStatusDescription(config, comapredDS, primary_key_column)

    # writing dataset into the destination
    generateReport(config, validatedDS)

    # noting the end time of the application
    endTime = datetime.now().time().strftime('%H:%M:%S')

    print("Job Elapsed Time (H:M:S): ",
          datetime.strptime(endTime, '%H:%M:%S') - datetime.strptime(startTime, '%H:%M:%S'))

    return None


def generateReport(config, validatedDS):
    # converting double to string type
    validatedDS = validatedDS.withColumn("total_amount_ecap", col("total_amount_ecap").cast(StringType()))\
        .withColumn("total_amount_lineitem",col("total_amount_lineitem").cast(StringType()))

    validatedDS = validatedDS.persist()

    ecapValidationResultPath = config['Ecap']['ecapValidationResultPath']
    parquetEcapValidationResultPath = config['Ecap']['parquetEcapValidationResultPath']

    writeIntoServingLayer(validatedDS, ecapValidationResultPath, "Overwrite")

    writeIntoParquetServingLayer(validatedDS, parquetEcapValidationResultPath, "Overwrite")

    validatedDS.unpersist()

    return None


def getServingData(config, primary_key_column, fromDate, toDate):
    servPath = config['InvoicePath']['servingPath']

    servingLayerColumns = config['Ecap']['servingLayerColumns']
    constServingLayerColumns = config['Ecap']['constServingLayerColumns']

    # reading the serving data
    serDS = createDatasetFromCSVFile(config, servPath).select(*servingLayerColumns)

    # renaming the columns, where we don't need to change if the columns in serving layer changes.
    serDS = serDS.toDF(*constServingLayerColumns)

    if (fromDate != '') | (toDate != ''):
        serDS = serDS.filter((col('invoice_date') >= lit(fromDate)) & (col('invoice_date') <= lit(toDate)))

    # summing the TOS amount
    serDS = serDS.withColumn('total_amount_lineitem',
                             sum(col('t_lineitem_amount')).over(Window.partitionBy(*primary_key_column))) \
        .withColumn('total_amount_lineitem', round(col('total_amount_lineitem'), 2).cast(DoubleType())) \
        .withColumn('t_lineitem_amount',
                    collect_list(col('t_lineitem_amount')).over(Window.partitionBy(*primary_key_column))) \
        .distinct()

    return serDS


def getEcapData(config, primary_key_column, fromDate, toDate):
    ecapPath = config["Master"]["ecapPath"]

    ecapColumns = config['Ecap']['ecapColumns']
    constEcapColumns = config['Ecap']['constEcapColumns']

    # reading the ecap data
    ecapDS = createDatasetFromCSVFile(config, ecapPath).select(*ecapColumns)

    # renaming the columns, where we don't need to change if the columns in ecap file changes.
    ecapDS = ecapDS.toDF(*constEcapColumns)

    if (fromDate != '') | (toDate != ''):
        ecapDS = ecapDS.filter((col('invoice_date') >= lit(fromDate)) & (col('invoice_date') <= lit(toDate)))

    # summing the amount for same invoice id
    ecapDS = ecapDS.withColumn('total_amount_ecap',
                               regexp_replace(col('total_amount_ecap'), ',', '').cast(DoubleType())) \
        .withColumn('total_amount_lineitem_ecap', col('total_amount_ecap')) \
        .withColumn('total_amount_ecap', round(
        sum(col('total_amount_ecap')).over(Window.partitionBy(*primary_key_column)), 2)) \
        .dropDuplicates(primary_key_column)

    return ecapDS


def compareServingEcapDS(config, serDS, ecapDS, primary_key_column):
    serving_InvoiceDate_Format = config['Ecap']['serving_InvoiceDate_Format']
    ecap_InvoiceDate_Format = config['Ecap']['ecap_InvoiceDate_Format']

    ecap_Col = ecapDS.columns
    ser_Col = serDS.columns

    # duplicate of invoice_id
    ecapDS = ecapDS.withColumn("Ecap_ID", col('invoice_id'))
    serDS = serDS.withColumn("Serving_ID", col('invoice_id'))

    joinedDS = serDS.join(ecapDS, *[primary_key_column], 'outer')  # "left_outer"

    # duplicate invoice_id as used to check the record missing in Ecap or in serving layer
    joinedDS = joinedDS.withColumn("ID_Desc",
                                   when(col('Ecap_ID').isNull() & col('Serving_ID').isNotNull(),
                                        'Record not found in ECAP')
                                   .when(col('Ecap_ID').isNotNull() & col('Serving_ID').isNull(),
                                         'Inovice not found')).drop('Serving_ID', 'Ecap_ID')

    # re-format the date
    joinedDS = joinedDS.withColumn("invoice_date", date_format(
        to_timestamp(col("invoice_date").cast(TimestampType()), serving_InvoiceDate_Format),
        'MM-dd-yyyy')) \
        .withColumn("invoice_date_ecap", date_format(
        unix_timestamp(col("invoice_date_ecap"), ecap_InvoiceDate_Format).cast(TimestampType()),
        'MM-dd-yyyy'))

    # filtering the pk columns from the list, ser_col n ecap_col has derived at the top
    for i in range(0, len(primary_key_column)):
        ser_Col = list(filter(lambda pcol: pcol != primary_key_column[i], ser_Col))
        ecap_Col = list(filter(lambda pcol: pcol != primary_key_column[i], ecap_Col))

    # columns not required for comparing
    ser_Col = list(filter(lambda pcol: pcol != 't_lineitem_amount', ser_Col))
    ser_Col = list(filter(lambda pcol: pcol != 'ID_Desc', ser_Col))

    ecap_Col.sort()
    ser_Col.sort()

    # comparing and generating the description for the each attribute
    for i in range(0, len(ser_Col)):
        joinedDS = joinedDS.withColumn(ser_Col[i] + "_zdesc",
                                       when((lower(col(ser_Col[i]))) == (lower(col(ecap_Col[i]))),
                                            "Matching") \
                                       .when(col(ser_Col[i]).isNull() & col(ecap_Col[i]).isNull(),
                                             "Not found on both") \
                                       .when(col(ser_Col[i]).isNull() & col(ecap_Col[i]).isNotNull(),
                                             "Missing in Invoice") \
                                       .when(col(ser_Col[i]).isNotNull() & col(ecap_Col[i]).isNull(),
                                             "Missing in Ecap") \
                                       .otherwise("MisMatch"))

    # split ecap shieel trip id by "/", if that first value contains in serving, then match
    joinedDS = joinedDS.withColumn('shell_trip_id_zdesc',
                                   when(col('shell_trip_id').contains(col('shell_trip_id_ecap')),
                                        "Matching")
                                   .otherwise(col('shell_trip_id_zdesc')))\
        .withColumn('shell_trip_id_zdesc',
                                   when(col('shell_trip_id').contains(split(col('shell_trip_id_ecap'), "/").getItem(0)),
                                        "Matching")
                                   .otherwise(col('shell_trip_id_zdesc')))

    # udf_Bplace = udf(compare, StringType())
    # joinedDS = joinedDS.withColumn('shell_trip_id_zdesc', udf_Bplace(col("shell_trip_id"),col("shell_trip_id_ecap"),col("shell_trip_id_zdesc")))

    joinedDS = joinedDS \
        .withColumn('total_amount_zdesc', when((col('total_amount_lineitem') != col('total_amount'))
                                               | (col('total_amount_lineitem').isNotNull() &
                                                  col('total_amount').isNull())
                                               | (col('total_amount_lineitem').isNull() &
                                                  col('total_amount').isNotNull()),
                                               lit('Invoice total and sum of lineitem mismatch'))
                    .otherwise(col('total_amount_zdesc'))) \
        .drop('total_amount_lineitem_ecap', 'total_amount_lineitem_zdesc')

    return joinedDS


def rowStatusDescription(config, comparedDS, primary_key_column):
    ser_ecap_Cols = comparedDS.columns

    for i in range(0, len(primary_key_column)):
        ser_ecap_Cols = list(filter(lambda pcol: pcol != primary_key_column[i], ser_ecap_Cols))

    ser_ecap_Cols.sort()

    desc_Cols = list(filter(lambda pcol: pcol.endswith("_zdesc"), comparedDS.columns))

    # list of columns to ignore
    columnsToIgnore = config['Ecap']['columnsToIgnore']

    # columns getting ignored
    for ignoredCol in range(0, len(columnsToIgnore)):
        comparedDS = comparedDS.withColumn(columnsToIgnore[ignoredCol] + "_zdesc", lit('Ignored'))

    comparedDS = comparedDS.withColumn("desc", lit(''))

    # adding the bad columns into description
    for i in range(0, len(desc_Cols)):
        comparedDS = comparedDS.withColumn("desc",
                                           when((col(desc_Cols[i]) == 'Matching')
                                                | (col(desc_Cols[i]) == 'Not found on both')
                                                | (col(desc_Cols[i]) == 'Ignored'),
                                                col('desc')).otherwise(
                                               concat(col('desc'), lit(' '), lit(desc_Cols[i]))))

    # elaborating the description
    comparedDS = comparedDS.withColumn("description",
                                       when(col('desc') != '',
                                            concat(regexp_replace(col('desc'), '_zdesc', ''), lit(' is not matching')))
                                       .otherwise(None))

    # adding status column.
    comparedDS = comparedDS.withColumn("status", when(col('description').isNotNull(), "Unmatched")
                                       .otherwise("Matched")) \
        .withColumn("status", when(col('ID_Desc').isNotNull(), col('ID_Desc')).otherwise(col("status")))

    # array to string, remove concat_ws function if you are pushing to database
    comparedDS = comparedDS.withColumn("t_lineitem_amount", concat_ws(',', col('t_lineitem_amount')))

    comparedDS = comparedDS.select('status', 'description', *primary_key_column, *ser_ecap_Cols).drop('ID_Desc')

    return comparedDS


def compare(shellID_Serving, shellID_Ecap, shellDesc):

    shellNewDesc = ""

    if (shellID_Serving is not None) and (shellID_Ecap is not None):
        if shellID_Ecap in shellID_Serving:
            shellNewDesc = "Matching"
        else :
            shellNewDesc = shellDesc

    return shellNewDesc

# entry point for PySpark Ecap Validation application
if __name__ == '__main__':
    ecapValidation()

#
# def ecapValidation(config):
#     servPath = config['InvoicePath']['servingPath']
#     ecapPath = config["Master"]["ecapPath"]
#
#     servingLayerColumns = ['InvoiceID', 'InvoiceDate', 'ShellTripID', 'TotalAmountDue', 'AmountDue']
#     constServingLayerColumns = ['invoice_id', 'invoice_date', 'shell_trip_id', 'total_amount', 't_lineitem_amount']
#     ecapColumns = ['INVOICE_NUMBER', 'INVOICE_DATE', 'TRIP_NUMBER', 'AMOUNT']
#     constEcapColumns = ['invoice_id', 'invoice_date_ecap', 'shell_trip_id_ecap', 'total_amount_ecap']
#
#     serving_InvoiceDate_Format = "MM/dd/yyyy"
#     ecap_InvoiceDate_Format = "dd-MMM-yy"
#
#     # mention the primary keyColumn
#     primary_key_column = ['invoice_id']
#
#     # reading the serving data
#     serDS = createDatasetFromCSVFile(config, servPath).select(*servingLayerColumns)
#
#     # renaming the columns, where we don't need to change if the columns in serving layer changes.
#     serDS = serDS.toDF(*constServingLayerColumns)
#
#     # summing the TOS amount
#     serDS = serDS.withColumn('total_amount_lineitem',
#                              sum(col('t_lineitem_amount')).over(Window.partitionBy(*primary_key_column))) \
#         .withColumn('total_amount_lineitem', round(col('total_amount_lineitem'), 2).cast(DoubleType())) \
#         .withColumn('t_lineitem_amount',
#                     collect_list(col('t_lineitem_amount')).over(Window.partitionBy(*primary_key_column))) \
#         .distinct()
#
#     # reading the ecap data
#     ecapDS = createDatasetFromCSVFile(config, ecapPath).select(*ecapColumns)
#
#     # renaming the columns, where we don't need to change if the columns in ecap file changes.
#     ecapDS = ecapDS.toDF(*constEcapColumns)
#
#     ecapDS = ecapDS.withColumn('total_amount_ecap',
#                                regexp_replace(col('total_amount_ecap'), ',', '').cast(DoubleType())) \
#         .withColumn('total_amount_lineitem_ecap', col('total_amount_ecap'))
#
#     joinedDS = serDS.join(ecapDS, *[primary_key_column], "left_outer")
#
#     joinedDS = joinedDS.withColumn("invoice_date",
#                                    to_timestamp(col("invoice_date").cast(TimestampType()),
#                                    serving_InvoiceDate_Format))\
#         .withColumn("invoice_date_ecap",
#                     unix_timestamp(col("invoice_date_ecap"), ecap_InvoiceDate_Format).cast(TimestampType()))
#
#     ecap_Col = ecapDS.columns
#     ser_Col = serDS.columns
#
#     for i in range(0, len(primary_key_column)):
#         ser_Col = list(filter(lambda pcol: pcol != primary_key_column[i], ser_Col))
#         ecap_Col = list(filter(lambda pcol: pcol != primary_key_column[i], ecap_Col))
#
#     ser_Col = list(filter(lambda pcol: pcol != 't_lineitem_amount', ser_Col))
#
#     ecap_Col.sort()
#     ser_Col.sort()
#
#     for i in range(0, len(ser_Col)):
#         joinedDS = joinedDS.withColumn(ser_Col[i] + "_zdesc",
#                                        when((lower(col(ser_Col[i]))) == (lower(col(ecap_Col[i]))),
#                                             "Matching") \
#                                        .when(col(ser_Col[i]).isNull() & col(ecap_Col[i]).isNull(),
#                                              "Empty") \
#                                        .when(col(ser_Col[i]).isNull() & col(ecap_Col[i]).isNotNull(),
#                                              "Extraneous") \
#                                        .when(col(ser_Col[i]).isNotNull() & col(ecap_Col[i]).isNull(),
#                                              "Missing") \
#                                        .otherwise("MisMatch"))
#
#     # keeping same description columns for total and lineitems amount and drop lineitem descp
#     joinedDS = joinedDS \
#         .withColumn('total_amount_zdesc', when(col('total_amount_lineitem_zdesc') == col('total_amount_zdesc'),
#                                                col('total_amount_zdesc'))
#                     .otherwise(concat(lit('lineitems_amount '), col('total_amount_lineitem_zdesc'),
#                                       lit(' and total_amount '), col('total_amount_zdesc')))) \
#         .drop('total_amount_lineitem_ecap', 'total_amount_lineitem_zdesc')
#
#     ser_ecap_Cols = joinedDS.columns
#
#     for i in range(0, len(primary_key_column)):
#         ser_ecap_Cols = list(filter(lambda pcol: pcol != primary_key_column[i], ser_ecap_Cols))
#
#     ser_ecap_Cols.sort()
#
#     desc_Cols = list(filter(lambda pcol: pcol.endswith("_zdesc"), joinedDS.columns))
#
#     joinedDS = joinedDS.withColumn("desc", lit(''))
#
#     for i in range(0, len(desc_Cols)):
#         joinedDS = joinedDS.withColumn("desc",
#                                        when((col(desc_Cols[i]) == 'Matching')
#                                             | (col(desc_Cols[i]) == 'Empty'),
#                                             col('desc')).otherwise(concat(col('desc'), lit(' '), lit(desc_Cols[i]))))
#
#     joinedDS = joinedDS.withColumn("description",
#                                    when(col('desc') != '',
#                                         concat(regexp_replace(col('desc'), '_zdesc', ''), lit(' is not matching')))
#                                    .otherwise(None))\
#         .withColumn("status", when(col('description').isNotNull() , "Unmatched").otherwise("Matched"))
#
#     # remove if you are pushing to database
#     joinedDS = joinedDS.withColumn("t_lineitem_amount", concat_ws(',' , col('t_lineitem_amount')))
#
#     joinedDS = joinedDS.select('status', 'description', *primary_key_column, *ser_ecap_Cols)
#
#     # joinedDS.show(500, truncate=False)
#
#     ecapValidationResultPath = '/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/Output/ecap'
#
#     writeIntoServingLayer(joinedDS, ecapValidationResultPath, "Overwrite")
#
#     return None
