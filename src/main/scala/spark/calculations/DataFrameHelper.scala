package spark.calculations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait DataFrameHelper {

  val subDivision = udf((division: String, group: String) => if (division == "46" || division == "47") if (group == "461" || group == "478" || group == "479") "A" else "B" else "C")

  private def prefixDivision(prefix: Int) = udf((column: String) => column.substring(0, prefix))

  def groupLEU(df: DataFrame): DataFrame = {
    df.groupBy("ern").agg(collect_list("ubrn").as("ubrns"))
  }

  def getClassification(df: DataFrame): DataFrame = {
    val sic1 = getSic(df.withColumn("division", prefixDivision(2)(col("sic07"))), "division")
    val sic2 = getSic(sic1.withColumn("subdivision", subDivision(col("division"), prefixDivision(3)(col("sic07")))),"subdivision")
    val sic3 = getSic(sic2.withColumn("division", prefixDivision(2)(col("sic07"))), "division")
    val sic4 = getSic(sic3.withColumn("group", prefixDivision(3)(col("sic07"))), "group")
    val sic5 = getSic(sic4.withColumn("class", prefixDivision(4)(col("sic07"))), "class")
    getSic(sic5, "sic07")
  }

  private def getSic(dataFrame: DataFrame, level: String): DataFrame = {
    val sumDF = dataFrame.groupBy("ern", level).agg(sum("employees") as s"sum_$level")
    val maxDF = sumDF.groupBy("ern").agg(max(s"sum_$level") as s"max_$level")
    dataFrame.join(sumDF, Seq("ern", level)).join(maxDF, "ern").filter(s"max_$level == sum_$level").drop(s"max_$level", s"sum_$level")
  }
}