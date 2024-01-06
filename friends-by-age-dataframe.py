from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("Friends by Age").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

age_friends = people.select("age","friends")
age_friends.groupBy("age").avg("friends").sort("age",ascending=False).show()
age_friends.groupBy("age").agg(func.sum("friends").alias("soma_amigos"),func.round(func.avg("friends"),2).alias("media_amigos")).sort("age",ascending=False).show()

spark.stop()

