package datasets

import helpers.Helpers.dfs_path
import org.apache.spark.sql._

object SparkSQLDataset {

    case class Person(userID: Int, name: String, age: Int, friends: Int)

    /** Our main function where the action happens */
    def main(args: Array[String]): Unit = {

        // Use SparkSession interface
        val spark = SparkSession
            .builder
            .appName("SparkSQL")
            .master("local[*]")
            .getOrCreate()

        // Load each line of the source data into an Dataset
        import spark.implicits._
        val schemaPeople = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(dfs_path + "fakefriends-header.csv")
            .as[Person]

        schemaPeople.printSchema()

        schemaPeople.createOrReplaceTempView("people")

        val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

        val results = teenagers.collect()

        results.foreach(println)

        spark.stop()
    }
}