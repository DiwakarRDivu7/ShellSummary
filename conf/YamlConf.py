"""


"""
import yaml

class YamlConf:
    def __init__(self):
        self._configYaml = None

    def setYamlConfig(self, confPath):
        configYaml : None
        with open(confPath) as file:
            configYaml = yaml.load(file, Loader=yaml.FullLoader)

        self._configYaml = configYaml

        return self._configYaml

    def getConfig(self):
        return self._configYaml