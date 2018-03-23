package dao.csv

/**
  * Created by chohab on 20/03/2018.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import global.Configs

object CsvDAO {

  import Configs._
  val logger = LoggerFactory.getLogger(getClass)

  def csvToParquet(implicit spark:SparkSession) {
    val leuToEnt = spark.read.option("header","true").csv(PATH_TO_CSV)
    leuToEnt.write.mode("overwrite").parquet(PATH_TO_PARQUET)
  }

}
