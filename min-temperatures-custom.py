from pyspark import SparkConf, SparkContext
import collections

def parseLine(line):
    field = line.split(",")

    station = field[0]
    observationType = field[2] 
    temperature = float(field[3]) * 0.1 * (9.0 / 5.0) + 32.0
    
    return (station,observationType,temperature)

conf = SparkConf().setMaster("local").setAppName("Min Temperatures")
sc = SparkContext(conf = conf)

lines = sc.textFile("1800.csv")

parsedLines = lines.map(parseLine)

#Only keeps TMIN
tmin = parsedLines.filter(lambda x: 'TMIN' in x[1])  #Only when True will pass to TMIN
tmax = parsedLines.filter(lambda x: 'TMAX' in x[1])  #Only when True will pass to TMIN

#Since its only TMIN we can remove obs type
stationTempsTMIN = tmin.map(lambda x:(x[0],x[2]))
stationTempsTMAX = tmax.map(lambda x:(x[0],x[2]))

#print("Current Dataset has:"+str(tmin.count())+" lines")

#This dataset now contens several tmin observations for every day of the year for every station.

minTemp = stationTempsTMIN.reduceByKey(lambda x,y:min(x,y)).collect()
maxTemp = stationTempsTMAX.reduceByKey(lambda x,y:max(x,y)).collect()

print("Temperatura Minima")

for result in minTemp:
    print(result[0] + "\t{:.2f}F".format(result[1]))

print("Temperatura Máxima")

for result in maxTemp:
    print(result[0] + "\t{:.2f}F".format(result[1]))


