"""
ShippingLoader.py
~~~~~~~~~~

comments

"""
import yaml
import sys
from datetime import datetime
from manager.processManager import ProcessManager


class ShippingLoader:
    configYaml = None

    def setYamlConfig(self, confPath):
        with open(confPath) as file:
            self.configYaml = yaml.load(file, Loader=yaml.FullLoader)

        return self.configYaml


    def getConfig(self):
        return self.configYaml


def main():
    try:
        startTime = datetime.now().time().strftime('%H:%M:%S')

        confPath = process = vendor = id = ""

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
        elif argLen == 5:
            confPath = sys.argv[1]
            process = sys.argv[2]
            vendor = sys.argv[3]
            id = sys.argv[4]

        # read yaml file
        config = ShippingLoader().setYamlConfig(confPath)

        # start process manager based on the arguments received.
        ProcessManager().initiateSelectedProcess(config, process, vendor, id)

        endTime = datetime.now().time().strftime('%H:%M:%S')

        print("Job Elapsed Time (H:M:S): ",
              datetime.strptime(endTime, '%H:%M:%S') - datetime.strptime(startTime, '%H:%M:%S'))
    except:
        raise

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
