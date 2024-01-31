from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

def computeCosineSimilarity(spark, data):
    # Compute xx, xy and yy columns
    pairScores = data \
      .withColumn("xx", func.col("rating1") * func.col("rating1")) \
      .withColumn("yy", func.col("rating2") * func.col("rating2")) \
      .withColumn("xy", func.col("rating1") * func.col("rating2")) 

    # Compute numerator, denominator and numPairs columns
    calculateSimilarity = pairScores \
      .groupBy("movie1", "movie2") \
      .agg( \
        func.sum(func.col("xy")).alias("numerator"), \
        (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"), \
        func.count(func.col("xy")).alias("numPairs")
      )

    # Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    result = calculateSimilarity \
      .withColumn("score", \
        func.when(func.col("denominator") != 0,func.col("numerator") / func.col("denominator")) \
          .otherwise(0) \
      ).select("movie1", "movie2", "score", "numPairs")

    return result

# Get movie name by given movie id 
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]

def getMovieGenres(movieId,movieNames):
    result = movieNames.filter(func.col("movieID") == movieId).head(1).collect()
    genres = []
    #for field in result:
    #  if 'genre' in field and int(result["field"]) == 1:
    #     genres.append(field)
    print(result)
    return genres.sort()
         


spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()


movieNamesSchema = StructType([ \
                               StructField("movieID", IntegerType(), True), \
                               StructField("movieTitle", StringType(), True), \
                               StructField("releaseDate", StringType(), True), \
                               StructField("imdbLink", StringType(), True), \
                               StructField("genre0", IntegerType(), True), \
                               StructField("genre1", IntegerType(), True), \
                               StructField("genre2", IntegerType(), True), \
                               StructField("genre3", IntegerType(), True), \
                               StructField("genre4", IntegerType(), True), \
                               StructField("genre5", IntegerType(), True), \
                               StructField("genre6", IntegerType(), True), \
                               StructField("genre7", IntegerType(), True), \
                               StructField("genre8", IntegerType(), True), \
                               StructField("genre9", IntegerType(), True), \
                               StructField("genre10", IntegerType(), True), \
                               StructField("genre11", IntegerType(), True), \
                               StructField("genre12", IntegerType(), True),\
                               StructField("genre13", IntegerType(), True), \
                               StructField("genre14", IntegerType(), True), \
                               StructField("genre15", IntegerType(), True), \
                               StructField("genre16", IntegerType(), True), \
                               StructField("genre17", IntegerType(), True), \
                               StructField("genre18", IntegerType(), True), \
                               StructField("genre19", IntegerType(), True) \
                               ]
                               )
    
moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])
    
    
# Create a broadcast dataset of movieID and movieTitle.
# Apply ISO-885901 charset
movieNames = spark.read \
      .option("sep", "|") \
      .option("charset", "ISO-8859-1") \
      .schema(movieNamesSchema) \
      .csv("ml-100k/u.item")

# Load up movie data as dataset
movies = spark.read \
      .option("sep", "\t") \
      .schema(moviesSchema) \
      .csv("ml-100k/u.data")


ratings = movies.select("userId", "movieId", "rating")

# Emit every movie rated together by the same user.
# Self-join to find every combination.
# Select movie pairs and rating pairs
moviePairs = ratings.alias("ratings1") \
      .join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId")) \
            & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
      .select(func.col("ratings1.movieId").alias("movie1"), \
        func.col("ratings2.movieId").alias("movie2"), \
        func.col("ratings1.rating").alias("rating1"), \
        func.col("ratings2.rating").alias("rating2"))


moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50.0

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter( \
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
          (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold)
          )

    # Sort by quality score.
    results = filteredResults.sort(func.col("score").desc()).take(5)
    
    print ("Top 10 similar movies for " + getMovieName(movieNames, movieID))
    
    for result in results:
        print(result[0])
        print("=================")
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
          similarMovieID = result.movie2
        
        if getMovieGenres(result[0],movieNames) != getMovieGenres(result[1],movieNames): continue
        
        print(getMovieName(movieNames, similarMovieID) + "\tscore: " \
              + str(result.score) + "\tstrength: " + str(result.numPairs))
        
