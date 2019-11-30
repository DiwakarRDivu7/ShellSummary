"""


"""
from transform.TransformBlesseySummary import TransformBlesseySummary
from transform.TransformKirbySummary import TransformKirbySummary
from validation.DataValidator import compareDS
from transform.mergeFile import mergeFiles


def initiateSelectedProcess(process, vendor=""):
    process = process.upper()

    if process == "TRANSFORMATION":
        ven = selectVendor(vendor)
        if ven == "incorrect vendor":
            raise("### specify the correct vendor in the 3rd argument, check the vendor name ###")
        else:
            ven.populateTable()
            # mergeFiles()

    elif process == "VALIDATION":
        compareDS()

    else:
        raise("### incorrect process, Avaialbe process are 'Transforamtion' and 'Validation' ###")

    return None


def selectVendor(vendor):
    vendor = vendor.upper()
    switcher = {
        "KIRBY": TransformKirbySummary(),
        "BLESSEY": TransformBlesseySummary()
    }

    return switcher.get(vendor, "incorrect vendor")
