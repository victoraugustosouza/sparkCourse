from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("Word Counts").getOrCreate()

book = spark.read.option("header", "false").option("inferSchema", "true")\
    .text("Book.txt")
    
words = book.select(func.explode(func.split(book.value,"\\W+")).alias("word"))
words = words.filter(words.word != '')

lowercase = words.select(func.lower("word").alias("words"))

wordCounts = lowercase.groupBy("words").agg(func.count("words").alias("countWords")).sort("countWords",ascending=False)

wordCounts.show(wordCounts.count())

#age_friends = people.select("age","friends")
#age_friends.groupBy("age").avg("friends").sort("age",ascending=False).show()
#age_friends.groupBy("age").agg(func.sum("friends").alias("soma_amigos"),func.round(func.avg("friends"),2).alias("media_amigos")).sort("age",ascending=False).show()

spark.stop()

