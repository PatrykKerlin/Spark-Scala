package datasets

import helpers.Helpers.dfs_path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/** Find the superhero with the most co-appearances. */
object MostObscureSuperheroes {

    private case class SuperHeroNames(id: Int, name: String)

    case class SuperHero(value: String)

    /** Our main function where the action happens */
    def main(args: Array[String]): Unit = {

        // Create a SparkSession using every core of the local machine
        val spark = SparkSession
            .builder
            .appName("MostPopularSuperhero")
            .master("local[*]")
            .getOrCreate()

        // Create schema when reading Marvel-names.txt
        val superHeroNamesSchema = new StructType()
            .add("id", IntegerType, nullable = true)
            .add("name", StringType, nullable = true)


        import spark.implicits._

        // Build up a hero ID -> name Dataset
        val names = spark.read
            .schema(superHeroNamesSchema)
            .option("sep", " ")
            .csv(dfs_path + "Marvel-Names.txt")
            .as[SuperHeroNames]

        val lines = spark.read
            .text(dfs_path + "Marvel-Graph.txt")
            .as[SuperHero]

        val connections = lines
            .withColumn("id", split(trim(col("value")), " ")(0))
            .withColumn("connections", size(split(trim(col("value")), " ")) - 1)
            .groupBy("id").agg(sum("connections").alias("connections"))

        val mostObscure = connections
            .agg(min($"connections")).first()(0)

        connections
            .filter($"connections" === mostObscure)
            .join(names, "id")
            .select("name")
            .show()

        spark.stop()
    }
}
