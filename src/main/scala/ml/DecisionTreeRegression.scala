package ml

import helpers.Helpers.dfs_path
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.SparkSession

object DecisionTreeRegression {

    private case class RealEstates(No: Int, TransactionDate: Double, HouseAge: Double, DistanceToMRT: Double,
                                   NumberConvenienceStores: Int, Latitude: Double, Longitude: Double,
                                   PriceOfUnitArea: Double)

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("DecisionTrees")
            .getOrCreate()

        import spark.implicits._

        val ds = spark.read
            .option("sep", ",")
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(dfs_path + "realestate.csv")
            .as[RealEstates]
            .select($"PriceOfUnitArea", $"HouseAge", $"DistanceToMRT", $"NumberConvenienceStores")

        val assembler = new VectorAssembler()
            .setInputCols(Array("HouseAge", "DistanceToMRT", "NumberConvenienceStores"))
            .setOutputCol("features")

        val df = assembler
            .transform(ds)
            .select($"PriceOfUnitArea", $"features")

        val trainTest = df.randomSplit(Array(0.75, 0.25))

        val trainDF = trainTest(0)
        val testDF = trainTest(1)

        val regressor = new DecisionTreeRegressor()
            .setFeaturesCol("features")
            .setLabelCol("PriceOfUnitArea")

        val model = regressor.fit(trainDF)

        val predictions = model.transform(testDF).cache()

        val result = predictions.select($"prediction", $"PriceOfUnitArea")

        result.show()

        spark.stop()
    }
}
