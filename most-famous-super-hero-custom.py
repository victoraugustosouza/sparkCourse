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
mostFamous = connections.orderBy(func.desc("conn")).first()
nameFamous = names.filter(func.col("idHero")==mostFamous["id"]).select("heroName").first()

print("Most Popular is "+nameFamous[0]+" with "+str(mostFamous[1])+" connections")
# sort by connection number descending, get the most connected
#vlookup Name.

# Stop the session
spark.stop()
