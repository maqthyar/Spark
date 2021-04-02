package dbBasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType, StringType, StructField, StructType}


object ImposingOwnSchema {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Creating Spark Context with Spark Session")
      .getOrCreate()

    //val file ="src/main/resources/baby-names-beginning-2007-1.csv"
    //var file = "src/main/resources/annual-enterprise-survey-2019-financial-year-provisional-csv.csv";
    //var file = "src/main/resources/sampleSubmission.csv"
    val file ="src/main/resources/sample.csv"


    val namesDf = spark.read
      .option("header","true")
      .option("inferschema", "true")
      .csv(file)

    namesDf.printSchema()

    println("Schema imposed is ")

    val ownSchema = StructType(
      StructField("year", FloatType, false) ::
        StructField("first_name", StringType, true) ::
        StructField("Country", StringType, true) ::
        StructField("Sex", StringType, true) ::
        StructField("Count", LongType, true) :: Nil
    )

    val namesDfwithOwnSchema = spark.read
      .option("header", "true")
      .schema(ownSchema)
      .option("mode","PERMISSIVE")   //PERMISSIVE DROPMALFORMED
      .csv(file)

    namesDfwithOwnSchema.printSchema()
    namesDfwithOwnSchema.show()
  }

}
