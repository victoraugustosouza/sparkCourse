from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType,LongType
import sys


def jaccardSimilarity(list1):
    global targetMovieList
    set1 = set(list1)
    set2 = set(targetMovieList)

    intersection_size = len(set1.intersection(set2))
    union_size = len(set1.union(set2))

    similarity = intersection_size / union_size if union_size != 0 else 0.0

    return similarity    


# Get movie name by given movie id 
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]


spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

movieNamesSchema = StructType([ \
                               StructField("movieID", IntegerType(), True), \
                               StructField("movieTitle", StringType(), True) \
                               ])
    
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

jaccardSimilarity_udf = func.udf(jaccardSimilarity, FloatType())

#Filtering out bad ratings
ratings = movies.filter(func.col("rating") > 2).select("userId", "movieId")

# Genereting list of users who watched every movie
listUsersWhowatched = ratings.groupBy("movieId").agg(func.collect_list("userId").alias("userList")).cache()

#Receving ID from movie we want recommendations
targetMovieID = int(sys.argv[1])

#Retrivieng users who watched movie of interest
targetMovieList = listUsersWhowatched.filter(func.col("movieId")==targetMovieID).first()["userList"]

#Filtering out list of users who watched movie of interest ( becasuse Jaccard Similarity would be 1)
listUsersWhowatched = listUsersWhowatched.filter(func.col("movieId") != targetMovieID)

#Calculating Jaccard Similarity e sorting by it
listUsersWhowatched = listUsersWhowatched.withColumn("jaccardSimilarity",jaccardSimilarity_udf(func.col("userList"))).sort(func.col("jaccardSimilarity").desc()).take(10)


print ("Top 10 similar movies for " + getMovieName(movieNames, targetMovieID),end="\n\n")


for result in listUsersWhowatched:
    # Display the similarity result that isn't the movie we're looking at
   
    print(getMovieName(movieNames, result.movieId) + "\tscore: " \
          + str(result.jaccardSimilarity))
        