from pyspark import SparkConf, SparkContext
import collections
import string
import re

def betterParseText(line):
    text = re.compile(r'\W+').split(line.lower())
    return text

conf = SparkConf().setMaster("local").setAppName("Word Counter")
sc = SparkContext(conf = conf)

lines = sc.textFile("Book.txt")
rdd_words = lines.flatMap(betterParseText)
wordCounts = rdd_words.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y)
flippedWordCount = wordCounts.map(lambda x: (x[1],x[0])).sortByKey(ascending=False)
results = flippedWordCount.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii','ignore')
    if word:
        print(word.decode() + ":\t\t" + count)
