package example

import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import org.apache.spark
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import org.apache.spark.sql.{SaveMode, SparkSession}


object ConstraintSuggestions {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("Creating Spark Context with Spark Session")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    //  val file = "src/main/resources/sample_aws.tsv"
    val file = "src/main/resources/employee.csv"

    val df = spark.read
      .option("header", "true")
      .csv(file)


    val suggestionResult = { ConstraintSuggestionRunner()
      .onData(df)
      .addConstraintRules(Rules.DEFAULT)
      .run()
    }

    import spark.implicits._

    val suggestionDataFrame = suggestionResult.constraintSuggestions.flatMap {
      case (column, suggestions) =>
        suggestions.map { constraint =>
          (column, constraint.description, constraint.codeForConstraint)
        }
    }.toSeq.toDS()

    suggestionDataFrame.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save("src/main/resources/output")


  }
}
