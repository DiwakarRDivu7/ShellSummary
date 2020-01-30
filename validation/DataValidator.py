"""
ShippingLoader.py
~~~~~~~~~~

comments
"""

from pyspark.sql.functions import when, lower, array_contains, array, to_timestamp
from connectors.TargetConnector import *
from pyspark.sql.types import *
from utils.DatasetUtils import getYamlConfig
import sys
from datetime import datetime


def compareDS():
    startTime = datetime.now().time().strftime('%H:%M:%S')

    confPath = ""

    argLen = len(sys.argv)

    # Check if received correct number of arguments.
    if argLen == 1:
        raise ("### configuration file path must be specified as argument ###")
    elif argLen == 2:
        confPath = sys.argv[1]

    # get the data from configuration file
    config = getYamlConfig(confPath)

    srcDSPath = config['Validation']['sourcePath']
    destDSPath = config['Validation']['targetPath']
    resultDSPath = config['Validation']['resultPath']

    # mention the primary keyColumn
    primary_key_column = config['Validation']['primaryKeyColumns']

    # list of columns to ignore
    columnsToIgnore = config['Validation']['columnsToIgnore']

    # reading files
    srcDS = createDatasetFromCSVFile(config, srcDSPath, "true")  # .filter("InvoiceID in ('1455659','1454503','1452733')")
    destDS = createDatasetFromCSVFile(config, destDSPath, "true") \
        .filter("InvoiceID in ('1463030','1466607','1465509','9300046511','9300046512','9300045339')")

    srcDS = srcDS.withColumn("amountdue", col("amountdue").cast(FloatType())) \
        .withColumn("invoicedate", to_timestamp(col("invoicedate"), "MM/dd/yyyy"))

    src_dest_DS = joinCompareAttr(srcDS, destDS, primary_key_column, columnsToIgnore)

    src_dest_DS = rowDescription(src_dest_DS, primary_key_column)

    # src_dest_DS.show(200, truncate=False)
    writeIntoServingLayer(src_dest_DS, resultDSPath, "Overwrite")

    endTime = datetime.now().time().strftime('%H:%M:%S')

    print("Job Elapsed Time (H:M:S): ",
          datetime.strptime(endTime, '%H:%M:%S') - datetime.strptime(startTime, '%H:%M:%S'))
    return None


def joinCompareAttr(src_DS, dest_DS, primary_key_column, columnsToIgnore):
    destDS = dest_DS
    srcDS = src_DS

    # droping columns, which does not required for comparision
    if ~(not columnsToIgnore):
        srcDS = srcDS.drop(*columnsToIgnore)
        destDS = destDS.drop(*columnsToIgnore)

    # converting all PK columns to lower case
    for x in range(0, len(primary_key_column)):
        srcDS = srcDS.withColumn(primary_key_column[x], lower(col(primary_key_column[x])))
        destDS = destDS.withColumn(primary_key_column[x], lower(col(primary_key_column[x])))

    destCols = destDS.columns

    for i in range(0, len(primary_key_column)):
        destCols = list(filter(lambda pcol: pcol != primary_key_column[i], destCols))

    # rename destDS columns
    for colName in destCols:
        destDS = destDS.withColumnRenamed(colName, (colName + "_obtained").lower())
        srcDS = srcDS.withColumnRenamed(colName, colName.lower())

    # joining the src and destination
    src_dest_joined = srcDS.join(destDS, *[primary_key_column], "outer").orderBy(primary_key_column)

    dest_Col = destDS.columns
    src_Col = srcDS.columns

    for i in range(0, len(primary_key_column)):
        src_Col = list(filter(lambda pcol: pcol != primary_key_column[i], src_Col))
        dest_Col = list(filter(lambda pcol: pcol != primary_key_column[i], dest_Col))

    dest_Col.sort()
    src_Col.sort()

    for i in range(0, len(src_Col)):
        src_dest_joined = src_dest_joined.withColumn(src_Col[i] + "_zdesc",
                                                     when((lower(col(src_Col[i]))) == (lower(col(dest_Col[i]))),
                                                          "Matching") \
                                                     .when(col(src_Col[i]).isNull() & col(dest_Col[i]).isNull(),
                                                           "EmptyValues") \
                                                     .when(col(src_Col[i]).isNull() & col(dest_Col[i]).isNotNull(),
                                                           "Extraneous") \
                                                     .when(col(src_Col[i]).isNotNull() & col(dest_Col[i]).isNull(),
                                                           "Missing") \
                                                     .otherwise("MisMatch"))

    return src_dest_joined


def rowDescription(srcDest_DS, primary_key_column):
    src_dest_DS = srcDest_DS

    src_dest_Cols = src_dest_DS.columns

    for i in range(0, len(primary_key_column)):
        src_dest_Cols = list(filter(lambda pcol: pcol != primary_key_column[i], src_dest_Cols))

    src_dest_Cols.sort()

    desc_Cols = list(filter(lambda pcol: pcol.endswith("_zdesc"), src_dest_DS.columns))

    src_dest_DS = src_dest_DS.withColumn("row_desc", when(array_contains(array(*desc_Cols), "MisMatch")
                                                          | array_contains(array(*desc_Cols), "Missing")
                                                          | array_contains(array(*desc_Cols), "Extraneous")
                                                          , "Bad_row").otherwise("Perfect_row")) \
        .withColumn("row_desc", when(array_contains(array(*desc_Cols), "Missing")
                                     & ~(array_contains(array(*desc_Cols), "MisMatch"))
                                     & ~(array_contains(array(*desc_Cols), "Extraneous"))
                                     & ~(array_contains(array(*desc_Cols), "Matching"))
                                     , "Missing_row").otherwise(col("row_desc"))) \
        .withColumn("row_desc", when(array_contains(array(*desc_Cols), "Extraneous")
                                     & ~(array_contains(array(*desc_Cols), "MisMatch"))
                                     & ~(array_contains(array(*desc_Cols), "Missing"))
                                     & ~(array_contains(array(*desc_Cols), "Matching"))
                                     , "Extraneous_row").otherwise(col("row_desc")))

    src_dest_DS = src_dest_DS.select("row_desc", *primary_key_column, *src_dest_Cols)

    return src_dest_DS

# entry point for PySpark validation application
if __name__ == '__main__':
    compareDS()