package datasets

import helpers.Helpers.dfs_path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.Codec

/** Find the movies with the most ratings. */
object PopularMovies {

    case class Movies(userID: Int, movieID: Int, rating: Int, timestamp: Long)

    /** Load up a Map of movie IDs to movie names. */
    private def loadMovieNames(spark: SparkSession): Map[Int, String] = {
        // Handle character encoding issues:
        // This is the current encoding of u.item, not UTF-8.
        implicit val codec: Codec = Codec("ISO-8859-1")

        // Read the movie names from HDFS using Spark
        val lines = spark.read
            .option("charset", "ISO-8859-1")
            .textFile(dfs_path + "ml-100k/u.item")

        // Process the lines to extract movie names and IDs
        // We use a flatMap to handle lines that may not parse correctly, discarding them
        val movies = lines.rdd.map { line =>
            val fields = line.split('|')
            if (fields.length > 1) {
                Some(fields(0).toInt -> fields(1))
            } else None
        }.collect().flatten.toMap // Collect the RDD to a Map

        movies
    }

    /** Our main function where the action happens */
    def main(args: Array[String]): Unit = {

        // Create a SparkSession using every core of the local machine
        val spark = SparkSession
            .builder
            .appName("PopularMovies")
            .master("local[*]")
            .getOrCreate()

        val nameDict = spark.sparkContext.broadcast(loadMovieNames(spark))

        // Create schema when reading u.data
        val moviesSchema = new StructType()
            .add("userID", IntegerType, nullable = true)
            .add("movieID", IntegerType, nullable = true)
            .add("rating", IntegerType, nullable = true)
            .add("timestamp", LongType, nullable = true)

        // Load up movie data as dataset
        import spark.implicits._
        val movies = spark.read
            .option("sep", "\t")
            .schema(moviesSchema)
            .csv(dfs_path + "ml-100k/u.data")
            .as[Movies]

        // Get number of reviews per movieID
        val movieCounts = movies.groupBy("movieID").count()

        // Create a user-defined function to look up movie names from our
        // shared Map variable.

        // We start by declaring an "anonymous function" in Scala
        val lookupName: Int => String = (movieID: Int) => {
            nameDict.value(movieID)
        }

        // Then wrap it with a udf
        val lookupNameUDF = udf(lookupName)

        // Add a movieTitle column using our new udf
        val moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(col("movieID")))

        // Sort the results
        val sortedMoviesWithNames = moviesWithNames.sort($"count".desc)

        // Show the results without truncating it
        sortedMoviesWithNames
            .select($"movieTitle").as("Title")
            .show(5, truncate = false)

        spark.stop()
    }
}

