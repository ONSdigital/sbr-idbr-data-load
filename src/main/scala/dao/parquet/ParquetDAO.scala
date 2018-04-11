package dao.parquet

import dao.hbase.converter.WithConversionHelper
import dao.hbase.HFileWriter
import global.Configs
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object ParquetDAO extends WithConversionHelper with  HFileWriter {

  import Configs._

  val logger = LoggerFactory.getLogger(getClass)

  def parquetToHFile(implicit spark: SparkSession) {

    val entRDD = spark.read.option("header", "true").csv(PATH_TO_ENT_CSV).rdd.map(row => toRecord(row, "ent")).cache
    val louRDD = spark.read.option("header", "true").csv(PATH_TO_LOU_CSV).rdd.map(row => toRecord(row, "lou")).cache

    toHFile(entRDD, PATH_TO_ENT_HFILE)
    toHFile(louRDD, PATH_TO_LOU_HFILE)
    toLinksHFile(entRDD, PATH_TO_LINKS_ENT_HFILE)
    toLinksHFile(louRDD, PATH_TO_LINKS_LOU_HFILE)

    entRDD.unpersist()
    louRDD.unpersist()
  }
}
