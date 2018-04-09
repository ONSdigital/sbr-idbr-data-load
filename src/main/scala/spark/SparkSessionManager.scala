package spark

import org.apache.spark.sql.SparkSession

trait SparkSessionManager {

  def withSpark(doWithinSparkSession: SparkSession => Unit) = {

    implicit val spark = SparkSession.builder().appName("idbr local unit assembler").getOrCreate()
      //.master("local[*]")

    doWithinSparkSession(spark)

    spark.stop()

  }
}
