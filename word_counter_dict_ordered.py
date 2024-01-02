from pyspark import SparkConf, SparkContext
import collections
import string
import re

def remove_punctuation(input_string):
    return ''.join(char for char in input_string if char not in string.punctuation)

def parseText(line):
    line = remove_punctuation(line)
    words = line.split(" ")
    upper_words = list(map(lambda x: x.upper(),words))
    return upper_words

def betterParseText(line):
    text = re.compile(r'\W+').split(line.lower())
    return text

conf = SparkConf().setMaster("local").setAppName("Word Counter")
sc = SparkContext(conf = conf)

lines = sc.textFile("Book.txt")
rdd_words = lines.flatMap(betterParseText)
wordCounts = rdd_words.countByValue()

sortedWordCount = sorted(wordCounts.items(), key=lambda x: x[1], reverse=True)

for word,count in sortedWordCount:
    cleanWord = word.encode('ascii','ignore')
    if cleanWord:
        print(cleanWord,count)