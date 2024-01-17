from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import functions as func


spark = SparkSession.builder.appName("Amout per customer").getOrCreate()



schema = StructType([
    StructField("clientId", StringType(), True),
    StructField("productId", IntegerType(), True),
    StructField("moneySpent", FloatType(), True)
    ])


storeData = spark.read.schema(schema).csv("customer-orders.csv")


    
print("Data Sample:")
storeData.show(5)

print("Data Schema:")
storeData.printSchema() 

storeData =storeData.select("clientID","moneySpent").groupBy("clientId").agg(func.round(func.sum("moneySpent"),2).alias("Money Spent"))
storeData = storeData.sort("Money Spent",ascending=False)
#storeData = storeData.withColumn("Money Spent",func.round("Money Spent",2)).sort("Money Spent",ascending=False)
storeData.show(storeData.count())
spark.stop()

