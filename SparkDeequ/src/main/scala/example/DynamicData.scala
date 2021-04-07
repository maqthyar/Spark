package example

import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.analyzers.{Analysis, ApproxCountDistinct, Completeness, InMemoryStateProvider, Size}
import com.amazon.deequ.examples.Item
import org.apache.spark.sql.SparkSession

object DynamicData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Analyzer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    //  val file = "src/main/resources/sample_aws.tsv"
    val file = "src/main/resources/baby-names-beginning-2007-1.csv"

    // data to run the analysis on
    val df = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .csv(file)

    val analysis = Analysis()
      .addAnalyzer(Size())
      .addAnalyzer(ApproxCountDistinct("id"))
      .addAnalyzer(Completeness("First Name"))
      .addAnalyzer(Completeness("County"))

    val stateStore = InMemoryStateProvider()

    val metricsForData = AnalysisRunner.run(data = df, analysis = analysis, saveStatesWith = Some(stateStore))

    println("Metrics for the first 3 records:\n")
    metricsForData.metricMap.foreach { case (analyzer, metric) =>
      println(s"\t$analyzer: ${metric.value.get}")
    }

    import spark.implicits._
    val moreData = Seq(
      (8, 2015, "John", "Washington", "M",30),
      (9, 2018, "Aaron", "Sydney", "F",25)
    ).toDF("id","Year","First Name","County","Sex","Count"
    )

    val metricsAfterAddingMoreData = AnalysisRunner.run(
      data = moreData,
      analysis = analysis,
      aggregateWith = Some(stateStore)
    )

    println("\nMetrics after adding 2 more records:\n")
    metricsAfterAddingMoreData.metricMap.foreach { case (analyzer, metric) =>
      println(s"\t$analyzer: ${metric.value.get}")
    }



  }

}
