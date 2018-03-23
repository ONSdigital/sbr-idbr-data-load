package spark

import org.apache.spark.sql.SparkSession

trait SparkSessionManager {

  def withSpark(doWithinSparkSession: SparkSession => Unit) = {

    implicit val spark = SparkSession.builder().appName("idbr enterprise assembler").getOrCreate()

    doWithinSparkSession(spark)

    spark.stop()

  }
}
