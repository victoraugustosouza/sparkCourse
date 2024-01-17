from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import functions as func


def CtoF(temperature):
    return float(temperature) * 0.1 * (9.0 / 5.0) + 32.0


spark = SparkSession.builder.appName("Min temp").getOrCreate()


CtoF_udf = func.udf(CtoF, returnType=FloatType())

schema = StructType([
    StructField("station", StringType(), True),
    StructField("day", IntegerType(), True),
    StructField("observationType", StringType(), True),
    StructField("temperature", IntegerType(), True)
])


temperatureData = spark.read.schema(schema).csv("1800.csv",schema=schema)


    
print("Data Sample:")
temperatureData.show(5)

temperatureData = temperatureData.select("station","observationType","temperature").withColumn("temperature", CtoF_udf("temperature"))

print("Data Sample After SELECT:")
temperatureData.show(5)

tmax = temperatureData.filter("observationType = 'TMAX'").select("station","temperature")
tmin = temperatureData.filter("observationType = 'TMIN'").select("station","temperature")

#temperatureData = temperatureData.filter(temperatureData["observationType"] == 'TMAX')
print("Data Sample after FILTER and SELECT:")
tmin.show(5)

print("Temperatura Máxima")

tmax.groupBy("station").agg(func.max("temperature").alias("Temperatura Máxima")).sort("Temperatura Máxima").show()
tmin.groupBy("station").agg(func.min("temperature").alias("Temperatura Minima")).sort("Temperatura Minima",ascending=False).show()
    
    
spark.stop()

