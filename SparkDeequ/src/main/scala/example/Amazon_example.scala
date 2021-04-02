package example

import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.{ApproxCountDistinct, Completeness, Compliance, Correlation, Mean, Size}
import org.apache.spark.sql.SparkSession

object Amazon_example {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("Creating Spark Context with Spark Session")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    val file = "src/main/resources/sample_aws.tsv"

    val df = spark.read
      .option("header","true")
      .option("delimiter", "\t")
      .csv(file)


    df.show(5)


    val analysisResult: AnalyzerContext = { AnalysisRunner
      // data to run the analysis on
      .onData(df)
      // define analyzers that compute metrics
      .addAnalyzer(Size())
      .addAnalyzer(Completeness("review_id"))
      .addAnalyzer(ApproxCountDistinct("review_id"))
      .addAnalyzer(Mean("star_rating"))
      .addAnalyzer(Compliance("top star_rating", "star_rating >= 4.0"))
      .addAnalyzer(Correlation("total_votes", "star_rating"))
      .addAnalyzer(Correlation("total_votes", "helpful_votes"))
      // compute metrics
      .run()
    }

    val metrics = successMetricsAsDataFrame(spark, analysisResult)

    metrics.show()

  }

}
