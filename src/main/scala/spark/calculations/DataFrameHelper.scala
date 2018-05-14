package spark.calculations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, max, sum, udf,collect_list}

trait DataFrameHelper {

  val subDivision = udf((division: String, group: String) => if (division == "46" || division == "47") if (group == "461" || group == "478" || group == "479") "A" else "B" else "C")

  private def prefixDivision(prefix: Int) = udf((column: String) => column.substring(0, prefix))

  def getSection(df1: DataFrame, df2: DataFrame): DataFrame = {
    val df = df1.join(df2, (df1("sic07") > df2("SICLower")) && (df1("sic07") < df2("SICUpper"))).drop("SICLower", "SICUpper")
    getClassification(df)
  }

  def groupLEU(df: DataFrame): DataFrame = {
    df.groupBy("ern").agg(collect_list("ubrn").as("ubrns"))
  }

  private def getClassification(df: DataFrame): DataFrame = {
    val sic1 = getSic(df.withColumn("division", prefixDivision(2)(col("sic07"))), "division").withColumn("subdivision", subDivision(col("division"), prefixDivision(3)(col("sic07"))))
    val sic2 = getSic(sic1.withColumn("division", prefixDivision(2)(col("sic07"))), "division")
    val sic3 = getSic(sic2.withColumn("group", prefixDivision(3)(col("sic07"))), "group")
    val sic4 = getSic(sic3.withColumn("class", prefixDivision(4)(col("sic07"))), "class")
    getSic(sic4, "sic07")
  }

  private def getSic(dataFrame: DataFrame, level: String): DataFrame = {
    val sumDF = if (level == "division2") dataFrame.groupBy("ern", level, "subdivision").agg(sum("employees") as s"sum_$level") else
      dataFrame.groupBy("ern", level).agg(sum("employees") as s"sum_$level")
    val maxDF = sumDF.groupBy("ern").agg(max(s"sum_$level") as s"max_$level")

    if (level == "division2") dataFrame.join(sumDF, Seq("ern", level, "subdivision")).join(maxDF, "ern").filter(s"max_$level == sum_$level").drop(s"max_$level", s"sum_$level") else
      dataFrame.join(sumDF, Seq("ern", level)).join(maxDF, "ern").filter(s"max_$level == sum_$level").drop(s"max_$level", s"sum_$level")
  }
}