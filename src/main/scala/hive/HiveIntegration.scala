package hive

import helpers.Helpers.dfs_path
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object HiveIntegration {

    private case class MovieTitles(movieID: Int, movieTitle: String)

    private case class MovieRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

    def main(args: Array[String]): Unit = {

        val startTimeMillis = System.currentTimeMillis()

        val spark = SparkSession.builder()
            .appName("HiveIntegration")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()

        import spark.implicits._

        spark.sql("CREATE DATABASE IF NOT EXISTS udemy")
        //        spark.sql("CREATE DATABASE IF NOT EXISTS global_temp")

        val movieTitlesTable = "udemy.MovieTitles"
        val movieRatingsTable = "udemy.MovieRatings"

        val movieTitlesExists = spark.catalog.tableExists(movieTitlesTable)
        val movieTitlesExistsAndNotEmpty = movieTitlesExists && !spark.table(movieTitlesTable).isEmpty

        if (!movieTitlesExistsAndNotEmpty) {

            val movieTitlesSchema: StructType = new StructType()
                .add("movieID", IntegerType, nullable = false)
                .add("movieTitle", StringType, nullable = false)

            val movieTitles = spark.read
                .option("sep", "::")
                .option("charset", "ISO-8859-1")
                .schema(movieTitlesSchema)
                .csv(dfs_path + "ml-1m/movies.dat")
                .as[MovieTitles]

            movieTitles.write.mode(SaveMode.Overwrite).saveAsTable(movieTitlesTable)
        }

        val movieRatingsExists = spark.catalog.tableExists(movieRatingsTable)
        val movieRatingsExistsAndNotEmpty = movieRatingsExists && !spark.table(movieRatingsTable).isEmpty

        if (!movieRatingsExistsAndNotEmpty) {

            val movieRatingsSchema: StructType = new StructType()
                .add("userID", IntegerType, nullable = false)
                .add("movieID", IntegerType, nullable = false)
                .add("rating", IntegerType, nullable = false)
                .add("timestamp", LongType, nullable = true)


            val movieRatings = spark.read
                .option("sep", "::")
                .schema(movieRatingsSchema)
                .csv(dfs_path + "ml-1m/ratings.dat")
                .as[MovieRatings]

            movieRatings.write.mode(SaveMode.Overwrite).saveAsTable(movieRatingsTable)
        }

        spark.sql("SELECT * FROM udemy.MovieTitles LIMIT 5").show()
        spark.sql("SELECT * FROM udemy.MovieRatings LIMIT 5").show()

        spark.stop()

        println(f"Time: ${(System.currentTimeMillis() - startTimeMillis) / 1000.0} seconds")
    }
}
