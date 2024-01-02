from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("ml-100k/u.data")
movies = lines.map(lambda x: x.split()[1])
result = movies.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("Movie: %s Number of ratings: %i" % (key, value))
