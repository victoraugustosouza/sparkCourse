from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import codecs

#init spark


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# load and broadcast(?) mapping ID/HERO dataset

schema = StructType([ \
                     StructField("idHero", IntegerType(), True), \
                     StructField("heroName", StringType(), True)
                     ]
)

names = spark.read.option("sep"," ").schema(schema).csv("Marvel+Names.txt")


# load connections data set as text, change it from to ID, connections number 

lines = spark.read.text("Marvel+Graph.txt")
connections = lines.withColumn("id",func.split("value"," ")[0]).withColumn("conn",func.size(func.split("value"," "))-1).groupBy("id").agg(func.sum("conn").alias("conn"))

minimumConnection = connections.agg(func.min("conn")).first()[0]
print("The minimun number of connections a super hero has is: "+str(minimumConnection))

lessFamous = connections.filter(func.col("conn")==minimumConnection)
lessFamousNames = lessFamous.join(names,names["idHero"]==lessFamous["id"],"left_outer").select("heroName")
lessFamousNames = lessFamousNames.collect()

print("All heros with "+ str(minimumConnection)+" connections are: \n")
for name in lessFamousNames:
    print(name["heroName"])

spark.stop()
