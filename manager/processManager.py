"""


"""
from transform_old.TransformBlesseySummary import TransformBlesseySummary
from transform_old.TransformKirbySummary import TransformKirbySummary
from validation.DataValidator import compareDS
from transform_old.mergeFile import mergeFiles


class ProcessManager:
    def initiateSelectedProcess(self, config, process, vendor="", id=''):
        process = process.upper()

        if process == "TRANSFORMATION":
            if vendor == "":
                raise ("### specify the vendor in the 3rd argument ###")
            else:
                vendorList = ProcessManager().getVendorsList(config, vendor)
                for vend in vendorList:
                    ven = ProcessManager().selectVendor(vend)
                    if ven == "incorrect vendor":
                        raise ("### specify the correct vendor in the 3rd argument, check the vendor name ###")
                    else:
                        print("### running " + vend + " vendor ###")
                        ven.populateTable(config, id)
                if id == "":
                    mergeFiles(self)

        elif process == "VALIDATION":
            compareDS(self, config)

        else:
            raise ("### incorrect process, Avaialbe process are 'Transformation' and 'Validation' ###")

        return None

    @staticmethod
    def selectVendor(vendor):
        vendor = vendor.upper()
        switcher = {
            "KIRBY": TransformKirbySummary(),
            "BLESSEY": TransformBlesseySummary()
        }

        return switcher.get(vendor, "incorrect vendor")

    @staticmethod
    def getVendorsList(config, vendor):
        vendor = vendor.upper()

        if vendor == "ALL":
            vendorList = config['Vendor']['All']
        elif "_" in vendor:  # vendor spliting by "_",  ex: kirby_blessey to ['Kirby',blessry']
            vendorList = vendor.split("_")
        else:  # string to array by spliting "###", ex: kirby to ['Kirby']
            vendorList = vendor.split("###")

        return vendorList
