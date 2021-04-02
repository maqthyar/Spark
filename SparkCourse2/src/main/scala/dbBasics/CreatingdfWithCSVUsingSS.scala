package dbBasics

import org.apache.spark.sql.SparkSession

object CreatingdfWithCSVUsingSS {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .master("local")
      .appName("Creating Spark Context with Spark Session")
      .getOrCreate()


    val properties = Map("header" -> "true", "inferSchema" -> "true")

    val file = "src/main/resources/annual-enterprise-survey-2019-financial-year-provisional-csv.csv"

    var df = spark.read.options(properties).csv(file)

    df.printSchema()

    df.show()

  }

}
