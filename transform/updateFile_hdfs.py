"""


"""
from connectors.TargetConnector import createDatasetFromCSVFile, writeIntoServingLayer, writeIntoParquetServingLayer
from pyspark.sql.functions import col, collect_set
import os
import shutil
from conf.session import singleSession
import subprocess


def updateFiles(config, vendorDS):
    servPath = config['InvoicePath']['servingPath']
    tempPath = config['InvoicePath']['servingTempPath']
    oldServPath = config['InvoicePath']['oldServingPath']
    newServingName = config['InvoicePath']['newServingName']
    parquetServingPath = config['InvoicePath']['parquetServingPath']

    check_dir = subprocess.Popen(["hadoop", "fs", "-test", "-e", oldServPath], stdout=subprocess.PIPE)
    wait = check_dir.wait()
    check = check_dir.returncode

    # if os.path.exists(oldServPath):
    if check == 0:
        # list of invoice id
        filterID = vendorDS.select(collect_set(col("_InvoiceNumber"))).first()[0]

        # read the temp file
        tempDS = createDatasetFromCSVFile(config, tempPath)

        # read the older serving file
        oldServDS = createDatasetFromCSVFile(config, oldServPath)

        # get all the invoice except the invoice need to update
        oldServDS = oldServDS.filter(~(col("InvoiceID").isin(*filterID)))

        DS = oldServDS.union(tempDS)
    else:
        DS = createDatasetFromCSVFile(config, tempPath)


    # writing into serving csv
    writeIntoServingLayer(DS, servPath, "Overwrite")

    if os.path.exists(oldServPath):
        serDS = createDatasetFromCSVFile(config, servPath)
        writeIntoServingLayer(serDS, oldServPath, "Overwrite")
        writeIntoParquetServingLayer(serDS, parquetServingPath, "Overwrite")
    else:
        writeIntoServingLayer(DS, oldServPath, "Overwrite")
        writeIntoParquetServingLayer(DS, parquetServingPath, "Overwrite")


    # # deleting temporary path
    # shutil.rmtree(tempPath)
    # # if os.path.exists(oldServPath):
    # #     shutil.rmtree(oldServPath)
    #
    # # renaming the serving csv file
    # file = os.listdir(servPath)
    # for files in file:
    #     if files.startswith("part"):
    #         os.rename(os.path.join(servPath,files), os.path.join(servPath,'InvoiceSummary.csv'))

    testNeed = config["test"]["testNeed"]
    if testNeed == 'yes':
        trail = config["test"]["trail"]
        uri = config["test"]["uri"]

        sc = singleSession().getSparkSession(config).sparkContext

        if trail == 1:
            subprocess.call(["hadoop", "fs", "-rm", "-r", oldServPath])
            subprocess.call(["hadoop", "fs", "-mv", servPath + "/part*", newServingName])
        if trail == 2:
            subprocess.call(["hadoop", "fs", "-rm", "-f", oldServPath])
        elif trail == 3:
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
            fs.delete(sc._jvm.org.apache.hadoop.fs.Path(oldServPath), True)
        elif trail == 4:
            URI = sc._gateway.jvm.java.net.URI
            Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
            FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
            fs = FileSystem.get(URI(uri), sc._jsc.hadoopConfiguration())
            fs.delete(Path(oldServPath))
        elif trail == 5:
            URI = sc._gateway.jvm.java.net.URI
            Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
            FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
            fs = FileSystem.get(URI(uri), sc._jsc.hadoopConfiguration())
            # We can now use the Hadoop FileSystem API (https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html)
            fs.listStatus(Path(oldServPath))


    return None
