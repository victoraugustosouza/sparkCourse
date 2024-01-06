from pyspark import SparkConf, SparkContext
import collections
import string
import re

def extractCustomerPricePair(line):
    fields = line.split(",")
    clientId = fields[0]
    moneySpent = float(fields[2])
    return (clientId,moneySpent)

conf = SparkConf().setMaster("local").setAppName("Customer Money")
sc = SparkContext(conf = conf)

input = sc.textFile("customer-orders.csv")
rdd = input.map(extractCustomerPricePair)
totalByClient = rdd.reduceByKey(lambda x,y: x+y)
orderedByAMount = totalByClient.map(lambda x: (x[1],x[0])).sortByKey(ascending=False)
results = orderedByAMount.collect()

for result in results:
    print(result[0],result[1])
