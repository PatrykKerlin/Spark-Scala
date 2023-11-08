package rdds

import helpers.Helpers.dfs_path
import org.apache.spark._

import scala.math.max

/** Find the maximum temperature by weather station for a year */
object MaxTemperatures {

    def parseLine(line: String): (String, String, Float) = {
        val fields: Array[String] = line.split(",")
        val stationID: String = fields(0)
        val entryType: String = fields(2)
        val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
        (stationID, entryType, temperature)
    }

    /** Our main function where the action happens */
    def main(args: Array[String]): Unit = {

        // Create a SparkContext using every core of the local machine
        val sc = new SparkContext("local[*]", "MaxTemperatures")

        val lines = sc.textFile(dfs_path + "1800.csv")
        val parsedLines = lines.map(parseLine)
        val maxTemps = parsedLines.filter(x => x._2 == "TMAX")
        val stationTemps = maxTemps.map(x => (x._1, x._3))
        val maxTempsByStation = stationTemps.reduceByKey((x, y) => max(x, y))
        val results = maxTempsByStation.collect()

        for (result <- results.sorted) {
            val station = result._1
            val temp = result._2
            val formattedTemp = f"$temp%.2f F"
            println(s"$station max temperature: $formattedTemp")
        }
    }
}