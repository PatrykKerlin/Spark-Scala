package rdds

import helpers.Helpers.dfs_path
import org.apache.spark._

/** Compute the total amount spent per customer in some fake e-commerce data. */
object TotalSpentByCustomer {

    /** Convert input data to (customerID, amountSpent) tuples */
    private def extractCustomerPricePairs(line: String): (Int, Float) = {
        val fields = line.split(",")
        (fields(0).toInt, fields(2).toFloat)
    }

    /** Our main function where the action happens */
    def main(args: Array[String]): Unit = {

        // Create a SparkContext using every core of the local machine
        val sc = new SparkContext("local[*]", "TotalSpentByCustomerSorted")

        val input = sc.textFile(dfs_path + "customer-orders.csv")

        val mappedInput = input.map(extractCustomerPricePairs)

        val totalByCustomer = mappedInput.reduceByKey((x, y) => x + y)

        val flipped = totalByCustomer.map(x => (x._2, x._1))

        val totalByCustomerSorted = flipped.sortByKey()

        val results = totalByCustomerSorted.collect()

        // Print the results.
        results.foreach(println)
    }

}

