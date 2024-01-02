from pyspark import SparkConf, SparkContext
import collections

def parseLine(line):
    field = line.split(",")
    age = int(field[2])
    n_friends = int(field[3])
    return (age,n_friends)

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

lines = sc.textFile("fakefriends.csv")
rdd = lines.map(parseLine)

#Pass every VALUE (not key!!) and transform it
totalsByAge=rdd.mapValues(lambda x:(x,1))
totalsByAge = totalsByAge.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
meanByAge = totalsByAge.mapValues(lambda x: x[0]/x[1]).sortByKey().collect()
for result in meanByAge:
    print(result)

#print(meanByAge.collect())
#sortedResults = collections.OrderedDict(sorted(rdd.items()))
#for key, value in sortedResults.items():
#    print("Movie: %s Number of ratings: %i" % (key, value))
