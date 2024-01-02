from pyspark import SparkConf, SparkContext
import collections
import string
import re

def parseLines(line):
    fields = line.split(",")
    clientId = fields[0]
    moneySpent = float(fields[2])
    return (clientId,moneySpent)

conf = SparkConf().setMaster("local").setAppName("Customer Money")
sc = SparkContext(conf = conf)

lines = sc.textFile("customer-orders.csv")
rdd = lines.map(parseLines)
totalByClient = rdd.reduceByKey(lambda x,y: x+y)
results = totalByClient.collect()

for result in results:
    print(result[0],result[1])
