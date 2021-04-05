package example

import scala.util.matching.Regex
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.ConstrainableDataTypes
import org.apache.spark.sql.{SaveMode, SparkSession}

object Constraints {

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

    val _check = Check(CheckLevel.Error, "Data Validation Check")
      .isComplete("First Name")
      .isComplete("Month Name of Joining")
      .isContainedIn("Month Name of Joining", Array("August", "July", "January", "April", "December", "November", "February", "March", "June", "September", "May", "October"))
      .isComplete("Phone_no")
      .containsEmail("E Mail")
      .containsSocialSecurityNumber("SSN")
      .isContainedIn("Day of Joining", 1, 31, includeLowerBound = true, includeUpperBound = true)
      .hasPattern("Phone_no", """^[+]*[(]{0,1}[0-9]{1,4}[)]{0,1}[-\s\./0-9]*$""".r)
      .hasDataType("Emp ID", ConstrainableDataTypes.Integral)
      .isUnique("Emp ID")

    val verificationResult: VerificationResult = {
      VerificationSuite()
        .onData(df)
        .addCheck(_check)
        .run()
    }
    val resultDataFrame = checkResultsAsDataFrame(spark, verificationResult)

    resultDataFrame.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save("src/main/resources/output")
  }
}
