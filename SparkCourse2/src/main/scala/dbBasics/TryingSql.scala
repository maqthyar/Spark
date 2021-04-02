package dbBasics

import org.apache.spark.sql.SparkSession


object TryingSql {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Creating Spark Context with Spark Session")
      .getOrCreate()

    val file ="src/main/resources/baby-names-beginning-2007-1.csv"
    //var file = "src/main/resources/annual-enterprise-survey-2019-financial-year-provisional-csv.csv";
    //var file = "src/main/resources/sampleSubmission.csv"
    //val file ="src/main/resources/sample.csv"

    val namesDf = spark.read
      .option("header","true")
      .option("inferschema", "true")
      .csv(file)

    namesDf.createOrReplaceTempView("children")
    val sqlDf = spark.sql("select * from children where Count > 200 and Sex = 'M' " )

//    Executing sql query
    sqlDf.show()

//    Sql queries inside filter function
    namesDf.groupBy("County").count().filter("County like '%s' ").show()


//    Functions inside the filter Function
    namesDf.groupBy("Year","County")
      .count()
      .orderBy("County")
      .filter(line =>
      { line.get(1) == "ALBANY" || line.get(1) == "ALLEGANY"  } )
      .show()


  }

}
