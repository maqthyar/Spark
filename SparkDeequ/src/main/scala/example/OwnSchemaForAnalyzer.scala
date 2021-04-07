package example

import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, LongType, StringType, StructField, StructType}


object OwnSchemaForAnalyzer {
  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
        .master("local")
        .appName("Analyzer")
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")


      //  val file = "src/main/resources/sample_aws.tsv"
      val file = "src/main/resources/baby-names-beginning-2007-1.csv"

    val ownSchema = StructType(
      StructField("id", StringType, false) ::
        StructField("year", FloatType, false) ::
        StructField("first_name", StringType, true) ::
        StructField("Country", StringType, true) ::
        StructField("Sex", StringType, true) ::
        StructField("Count", LongType, true) :: Nil
    )

      // data to run the analysis on


      val df = spark.read
        .option("header", "true")
        .option("inferschema", "true")
        //.schema(ownSchema)
        .csv(file)

    df.show()

      val analysisResult: AnalyzerContext = { AnalysisRunner
      .onData(df)
      .addAnalyzer(Mean("Count"))
      .addAnalyzer(Compliance("Count ", "Count > 200",None))
      .addAnalyzer(Correlation("Count", "year"))
      // compute metrics
      .run()
    }
    // retrieve successfully computed metrics as a Spark data frame

    analysisResult.metricMap.foreach { case (analyzer, metric) =>
      println(s"\t$analyzer: ${metric.value.get}")
    }

    /*val metrics = successMetricsAsDataFrame(spark, analysisResult)
    metrics.show(truncate = false)*/


  }
}
