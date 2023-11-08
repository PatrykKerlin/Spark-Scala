package rdds

import helpers.Helpers.dfs_path
import org.apache.spark._

/** Count up how many of each word appears in a book as simply as possible. */
object WordCount {

    /** Our main function where the action happens */
    def main(args: Array[String]): Unit = {

        // Create a SparkContext using every core of the local machine
        val sc = new SparkContext("local[*]", "WordCount")

        // Read each line of my book into an RDD
        val input = sc.textFile(dfs_path + "/book.txt")

        // Split into words separated by a space character
        val words = input.flatMap(x => x.split(" "))

        // Count up the occurrences of each word
        val wordCounts = words.countByValue()

        // Print the results.
        wordCounts.foreach(println)
    }

}

