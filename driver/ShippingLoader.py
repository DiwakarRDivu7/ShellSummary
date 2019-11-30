"""
ShippingLoader.py
~~~~~~~~~~

comments

"""
from manager.processManager import initiateSelectedProcess
import sys
# from transform.TransformInvoiceSummary import populateTable
# from transform.TransfromInvoice import populateTable

from datetime import datetime


def main():
    try:
        startTime = datetime.now().time().strftime('%H:%M:%S')

        # populateTable()

        confPath = ""
        process = ""
        vendor = ""

        argLen = len(sys.argv)

        # Check if received correct number of arguments.
        if argLen == 1:
            raise ("### configuration file path, process and vendor must be specified as argument ###")
        elif argLen == 2:
            raise ("### process and vendor must be specified as 2nd and 3rd argument respectively ###")
        elif argLen == 3:
            confPath = sys.argv[1]
            process = sys.argv[2]
        elif argLen == 4:
            confPath = sys.argv[1]
            process = sys.argv[2]
            vendor = sys.argv[3]

        # start process manager based on the arguments received.
        initiateSelectedProcess(process, vendor)

        endTime = datetime.now().time().strftime('%H:%M:%S')

        print("Job Elapsed Time (H:M:S): ",
              datetime.strptime(endTime, '%H:%M:%S') - datetime.strptime(startTime, '%H:%M:%S'))
    except:
        raise

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
