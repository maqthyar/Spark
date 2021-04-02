package example

import org.apache.spark.sql.SparkSession
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.{Compliance, Correlation, Size, Completeness, Mean, ApproxCountDistinct}
import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel}

object Deequ_example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Creating Spark Context with Spark Session")
      .getOrCreate()


//    val file = "src/main/resources/sample_aws.tsv"
    val file = "src/main/resources/baby-names-beginning-2007-1.csv"

    /*val df =   spark.read.option("header","true")
      .format("org.apache.spark.sql.execution.datasources.v2.csv.CSVDataSourceV2")
        .csv(file);*/

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .option("header","true")
//      .option("delimiter", "\t")
      .csv(file)


    //df.show(5)


    val verificationResult: VerificationResult = { VerificationSuite()
      // data to run the verification on
      .onData(df)
      // define a data quality check
      .addCheck(
        Check(CheckLevel.Error, "Review Check")
          .hasSize(_ >= 1000) // at least 1000 rows
          .hasMin("Year", _ == 1.0) // min is 1.0
          .hasMax("Year", _ == 5.0) // max is 5.0
          .isComplete("First Name") // should never be NULL
          .isUnique("Count") // should not contain duplicates
//          .isContainedIn("marketplace", Array("US", "UK", "DE", "JP", "FR"))
          .isNonNegative("Year")) // should not contain negative values
      // compute metrics and verify check conditions
      .run()
    }

    // convert check results to a Spark data frame
    val resultDataFrame = checkResultsAsDataFrame(spark, verificationResult)

//    resultDataFrame.show(truncate = false)
    resultDataFrame.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("src/main/resources/output.csv")

  }
}
