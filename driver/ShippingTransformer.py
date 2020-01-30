"""
ShippingTransformer.py~~~~~~~~~~

comments

"""
import yaml
import sys
from datetime import datetime
from manager.processController import initiateProcess
from conf.YamlConf import YamlConf


def main():
    try:
        startTime = datetime.now()
        sTime = startTime.time().strftime('%H:%M:%S')

        confPath = ""

        argLen = len(sys.argv)

        # Check if received correct number of arguments.
        if argLen == 1:
            raise ("### configuration file path must be specified as argument ###")
        elif argLen >= 2:
            confPath = sys.argv[1]

        # read yaml file
        config = YamlConf().setYamlConfig(confPath)

        # start process manager based on the arguments received.
        initiateProcess(config, startTime)

        endTime = datetime.now().time().strftime('%H:%M:%S')

        print("Job Elapsed Time (H:M:S): ",
              datetime.strptime(endTime, '%H:%M:%S') - datetime.strptime(sTime, '%H:%M:%S'))


    except:
        raise

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
