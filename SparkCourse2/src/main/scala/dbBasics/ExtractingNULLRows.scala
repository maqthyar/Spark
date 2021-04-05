package dbBasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object ExtractingNULLRows {

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


    val ownSchema = StructType(
      StructField("year", FloatType, false) ::
        StructField("First", StringType, true) ::
        StructField("Country", StringType, true) ::
        StructField("Sex", StringType, true) ::
        StructField("Count", LongType, true) :: Nil
    )

    val permissiveDF = spark.read
      .option("header", "true")
      .schema(ownSchema)
      .option("mode","DROPMALFORMED")   //PERMISSIVE DROPMALFORMED
      .csv(file)

    namesDf.createOrReplaceTempView("children")
    permissiveDF.createOrReplaceTempView("childrenWithNull")

    val df = spark.sql("select * from children c left join childrenWithNull c1 on  c.First_Name = c1.First");

    df.createOrReplaceTempView("temp")

    df.filter("c1.First is NULL").show()




  }

}
