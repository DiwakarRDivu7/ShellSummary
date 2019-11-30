"""
ShippingLoader.py
~~~~~~~~~~

comments
"""
from typing import List, Any

from pyspark.sql.functions import when, lower, col, array_contains, array
from connectors.TargetConnector import *
from pyspark.sql.types import *
import sys

def compareDS():

    print("executed validation" )
    sys.exit(9)

    srcDSPath = "/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/Compare_DataValidation/SourceDS.csv"
    destDSPath = "/Users/diwr/Desktop/Shell/Shell Doc/ShellShipping/Compare_DataValidation/DestinationDS.csv"

    srcDS = createDatasetFromCSVFile(srcDSPath).filter("InvoiceID in ('1455659','1454503','1452733')")
    destDS = createDatasetFromCSVFile(destDSPath).filter("InvoiceID in ('1455659','1454503','1452733')")

    #mention the primary keyColumn
    primary_key_column = ["InvoiceID","TypeOfService"]

    for x in range(0,len(primary_key_column)):
        # if (srcDS.schema[primary_key_column[x]].dataType != StringType()):
            srcDS = srcDS.withColumn(primary_key_column[x],lower(col(primary_key_column[x])))
            destDS = destDS.withColumn(primary_key_column[x],lower(col(primary_key_column[x])))

    src_dest_DS = joinCompareAttr(srcDS, destDS, primary_key_column)

    rowDescription(src_dest_DS, primary_key_column)

    return None


def joinCompareAttr(src_DS, dest_DS, primary_key_column):

    destDS = dest_DS
    srcDS = src_DS

    destCols = destDS.columns
    # srcCols = srcDS.columns

    for i in range(0,len(primary_key_column)):
        destCols = list(filter(lambda pcol: pcol != primary_key_column[i], destCols))
        # srcCols = list(filter(lambda pcol: pcol != primary_key_column[i], srcCols))

    #rename destDS columns
    for colName in destCols:
        destDS = destDS.withColumnRenamed(colName, (colName+"_obtained").lower())
        srcDS = srcDS.withColumnRenamed(colName, (colName).lower())

    #joining the src and destination
    src_dest_joined = srcDS.join(destDS, *[primary_key_column], "outer").orderBy(primary_key_column)

    dest_Col = destDS.columns
    src_Col = srcDS.columns

    for i in range(0,len(primary_key_column)):
        src_Col = list(filter(lambda pcol: pcol != primary_key_column[i], src_Col))
        dest_Col = list(filter(lambda pcol: pcol != primary_key_column[i], dest_Col))

    dest_Col.sort()
    src_Col.sort()

    for i in range(0,len(src_Col)):
            src_dest_joined = src_dest_joined.withColumn(src_Col[i]+"_zdesc", when( lower(col(src_Col[i])) == lower(col(dest_Col[i])), "Matching")\
                .when( col(src_Col[i]).isNull() & col(dest_Col[i]).isNull(), "EmptyValues")\
                .when( col(src_Col[i]).isNull() & col(dest_Col[i]).isNotNull(), "Extraneous")\
                .when( col(src_Col[i]).isNotNull() & col(dest_Col[i]).isNull(), "Missing")\
                .otherwise("MisMatch"))

    return src_dest_joined


def rowDescription(srcDest_DS, primary_key_column):

    src_dest_DS = srcDest_DS

    src_dest_Cols = src_dest_DS.columns

    for i in range(0,len(primary_key_column)):
        src_dest_Cols = list(filter(lambda pcol: pcol != primary_key_column[i], src_dest_Cols))

    src_dest_Cols.sort()

    desc_Cols = list(filter(lambda pcol: pcol.endswith("_zdesc"), src_dest_DS.columns))

    src_dest_DS = src_dest_DS.withColumn("row_desc", when(array_contains(array(*desc_Cols),"MisMatch")
                                                          | array_contains(array(*desc_Cols),"Missing")
                                                          | array_contains(array(*desc_Cols),"Extraneous")
                                                          ,"Bad_row").otherwise("Perfect_row")) \
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

    src_dest_DS.select("row_desc", *primary_key_column, *src_dest_Cols).show(truncate=False)

    return None
