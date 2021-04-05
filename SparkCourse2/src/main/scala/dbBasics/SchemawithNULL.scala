package dbBasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object SchemawithNULL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Creating Spark Context with Spark Session")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    //val file ="src/main/resources/baby-names-beginning-2007-1.csv"
    //var file = "src/main/resources/annual-enterprise-survey-2019-financial-year-provisional-csv.csv";
    //var file = "src/main/resources/sampleSubmission.csv"
    val file ="src/main/resources/sample.csv"


    val namesDf = spark.read
      .option("header","true")
      .option("inferschema", "true")
      .csv(file)

    println("without external schema")
    namesDf.printSchema()
    namesDf.show()

    val ownSchema = StructType(
      StructField("year", StringType, false) ::
        StructField("first_name", StringType, true) ::
        StructField("Country", StringType, true) ::
        StructField("Sex", StringType, true) ::
        StructField("Count", IntegerType, true) :: Nil
    )

    val namesDfwithOwnSchema = spark.createDataFrame(namesDf.rdd, ownSchema)

    println("Imposed one is")

    namesDfwithOwnSchema.printSchema()
    namesDfwithOwnSchema.show()
  }

}
