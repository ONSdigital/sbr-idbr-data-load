package spark

import org.apache.spark.sql.SparkSession



trait SparkSessionManager {

  def withSpark(doWithinSparkSession: SparkSession => Unit) = {

    implicit val spark: SparkSession = SparkSession.builder().appName("enterprise assembler").getOrCreate()

    doWithinSparkSession(spark)

    spark.stop()

  }
}
