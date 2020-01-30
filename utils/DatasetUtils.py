"""
DatasetUtils.py
~~~~~~~~

comments
"""
from connectors.TargetConnector import *
from pyspark.sql.types import StringType, StructField, StructType
from datetime import datetime
import yaml


def getYamlConfig(confPath):
    with open(confPath) as file:
        configYaml = yaml.load(file, Loader=yaml.FullLoader)

    return configYaml

def getOriginDestination(dataSet):
    return dataSet.withColumn("Origin", col("Origins").getField("country")) \
        .withColumn("OriginCity", col("Origins").getField("city")) \
        .withColumn("OriginState", col("Origins").getField("state")) \
        .withColumn("Destination", col("Destinations").getField("country")) \
        .withColumn("DestinationCity", col("Destinations").getField("city")) \
        .withColumn("DestinationState", col("Destinations").getField("state"))

def currentDate():
    return datetime.date.today()


def get_custom_service_price(serv, price):
    pair = ""
    if serv is None or price is None:
        ""
    else:
        if len(serv) == len(price):
            for i in range(0, len(serv)):
                pair = pair + serv[i].strip() + "<>" + str(price[i]) + ","
        else:
            print("length doesn't match")
    return pair[:-1]


# def get_kirby_country_city_state(place):
#     country = city = state = ""
#     arrPlace = place.split("/")
#
#     if arrPlace is None:
#         ""
#     else:
#         if len(arrPlace) == 2:
#             country = arrPlace[0]
#             city = arrPlace[1][0:-3]
#             state = arrPlace[1][-2:]
#         elif len(arrPlace) == 3:
#             country = arrPlace[0]
#             city = arrPlace[1]
#             state = arrPlace[2]
#         else:
#             country = place
#
#     return country, city, state

def get_kirby_country_city_state(place):
    country = city = state = ""

    if place is not None:
        if "," in place:
            multiplePlace = place.split(",")[0]
        else:
            multiplePlace = place

        arrPlace = multiplePlace.split("/")

        if len(arrPlace) == 2:
            country = arrPlace[0]
            city = arrPlace[1][0:-3]
            state = arrPlace[1][-2:]
        elif len(arrPlace) == 3:
            country = arrPlace[0]
            city = arrPlace[1]
            state = arrPlace[2]
        else:
            country = place

    return country, city, state


def get_blessey_country_city_state(placees):
    lis = lis1 = lis2 = ["", "", "", ""]
    place = placees

    if place == None:
        if place[-1] == ".":
            place = place[:-1]
    if "/" in place:
        arr = place.split("/")  # Ergon~Marietta~ OH/Enlink~Bells Run~ OH
        arr0 = arr[0].split("~")
        if len(arr0) < 4:
            for x in range(0, len(arr0)):
                lis[x] = arr0[x]  # first list contains, first set upto /
                # print(lis)
        arr1 = arr[1].split("~")
        if len(arr1) < 4:
            for y in range(0, len(arr1)):  # if(y<3):
                lis1[y] = arr1[y]  # second list contains, second set upto /
                # print(lis1)
        if len(arr) == 3:
            arr2 = arr[2].split("~")
            if len(arr2) < 4:
                for z in range(0, len(arr2)):
                    lis2[z] = arr2[z]  # third list contains, third set
                    # print(lis2)
    elif place == "TBN":
        for t in range(0, 3):
            lis[t] = "TBN"
            # print(lis)
    else:
        ar = place.split("~")  # Enlink~Bells Run~ OH  = direct, which doesn't have second place
        if ar is None:
            ""
        else:
            if len(ar) == 3:
                for x in range(0, len(ar)):
                    lis[x] = ar[x]
                    # print(lis)
            elif len(ar) == 2:
                st = ar[1].strip()  # check its state or not, triming for white space
                if len(st) == 2:
                    lis[1] = ar[0]
                    lis[2] = st
                else:
                    lis[0] = ar[0]
                    lis[1] = st

    # replace if city and state is empty by other list
    country = lis[0]
    city = lis[1]
    state = lis[2]
    if city == "":
        city = lis1[1]
        state = lis1[2]

    if state == "":
        state = lis2[2]

    if "." in city:
        if city[-3] == "." and state == "":  # Houston.Tx
            cs = city.split(".")
            city = cs[0]
            state = cs[1]
        elif "St." in city or "ST." in city:  # to remove "." from St.
            city = city.replace(".", "")

    if country == "" and city == "" and state == "":
        country = placees

    return country, city, state


schemaPlace = StructType([
    StructField("country", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False)
])
