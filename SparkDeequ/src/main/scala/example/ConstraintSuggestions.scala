package example

import org.apache.spark
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import org.apache.spark.sql.SparkSession


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

  }
}
