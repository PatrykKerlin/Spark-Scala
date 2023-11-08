package datasets

import helpers.Helpers.dfs_path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{round, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

/** Compute the total amount spent per customer in some fake e-commerce data. */
object TotalSpentByCustomer {

    private case class CustomerOrders(cust_id: Int, item_id: Int, amount_spent: Double)

    /** Our main function where the action happens */
    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder
            .appName("TotalSpentByCustomer")
            .master("local[*]")
            .getOrCreate()

        // Create schema when reading customer-orders
        val customerOrdersSchema = new StructType()
            .add("cust_id", IntegerType, nullable = true)
            .add("item_id", IntegerType, nullable = true)
            .add("amount_spent", DoubleType, nullable = true)

        // Load up the data into spark dataset
        // Use default separator (,), load schema from customerOrdersSchema and force case class to read it as dataset
        import spark.implicits._
        val customerDS = spark.read
            .schema(customerOrdersSchema)
            .csv(dfs_path + "customer-orders.csv")
            .as[CustomerOrders]

        val totalByCustomer = customerDS
            .groupBy("cust_id")
            .agg(round(sum("amount_spent"), 2)
                     .alias("total_spent"))

        val totalByCustomerSorted = totalByCustomer.sort("total_spent")

        totalByCustomerSorted.show(totalByCustomer.count.toInt)

        spark.stop()
    }
}

