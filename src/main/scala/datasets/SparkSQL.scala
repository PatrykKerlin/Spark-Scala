package datasets

import helpers.Helpers.dfs_path
import org.apache.spark.sql._

object SparkSQL {

    case class Person(ID: Int, name: String, age: Int, numFriends: Int)

    private def mapper(line: String): Person = {
        val fields = line.split(',')

        val person: Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
        person
    }

    /** Our main function where the action happens */
    def main(args: Array[String]): Unit = {

        // Use new SparkSession interface in Spark 2.0
        val spark = SparkSession
            .builder
            .appName("SparkSQL")
            .master("local[*]")
            .getOrCreate()

        val lines = spark.sparkContext.textFile(dfs_path + "fakefriends.csv")
        val people = lines.map(mapper)

        // Infer the schema, and register the DataSet as a table.
        import spark.implicits._
        val schemaPeople = people.toDS

        schemaPeople.printSchema()

        schemaPeople.createOrReplaceTempView("people")

        // SQL can be run over DataFrames that have been registered as a table
        val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

        val results = teenagers.collect()

        results.foreach(println)

        spark.stop()
    }
}