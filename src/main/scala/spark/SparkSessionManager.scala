package spark

import global.Configs.SPARK_DEPLOYMENT_MODE
import org.apache.spark.sql.SparkSession

trait SparkSessionManager {

  def withSpark(doWithinSparkSession: SparkSession => Unit) = {

    implicit val spark = SparkSession.builder().appName("idbr local unit assembler").getOrCreate()

    doWithinSparkSession(spark)

    spark.stop()

  }
}
