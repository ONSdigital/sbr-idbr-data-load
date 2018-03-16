package spark

import global.Configs.SPARK_DEPLOYMENT_MODE
import org.apache.spark.sql.SparkSession

trait SparkSessionManager {

  def withSpark(doWithinSparkSession: SparkSession => Unit) = {

    implicit val spark = if (SPARK_DEPLOYMENT_MODE == "cluster") SparkSession.builder().appName("idbr enterprise assembler").getOrCreate()
    else SparkSession.builder().master("local[4]").appName("idbr enterprise assembler").getOrCreate()

    doWithinSparkSession(spark)

    spark.stop()

  }
}
