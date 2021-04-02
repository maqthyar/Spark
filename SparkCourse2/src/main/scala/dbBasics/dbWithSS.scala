package dbBasics

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object dbWithSS {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Creating Spark Context with Spark Session")
      .getOrCreate()

    val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val schema = StructType(
      StructField("Integers as String", IntegerType, true) :: Nil
    )

    val arrayRDD = sparkSession.sparkContext.parallelize(array, 4)

    val rowRDD = arrayRDD.map(line => Row(line))

    val df = sparkSession.createDataFrame(rowRDD, schema);

    df.printSchema();
    df.show()
  }
}
