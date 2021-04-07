package example

import org.apache.spark.sql.SparkSession

object DataProfiling {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Analyzer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    /*case class RawData(productName: String, totalNumber: String, status: String, valuable: String)

    val rows = spark.sparkContext.parallelize(Seq(
      RawData("thingA", "13.0", "IN_TRANSIT", "true"),
      RawData("thingA", "5", "DELAYED", "false"),
      RawData("thingB", null,  "DELAYED", null),
      RawData("thingC", null, "IN_TRANSIT", "false"),
      RawData("thingD", "1.0",  "DELAYED", "true"),
      RawData("thingC", "7.0", "UNKNOWN", null),
      RawData("thingC", "20", "UNKNOWN", null),
      RawData("thingE", "20", "DELAYED", "false")
    ))

    val rawData = spark.createDataFrame(rows)*/
  }

}
