package dao.csv

import dao.hbase.converter.WithConversionHelper
import spark.calculations.DataFrameHelper
import dao.hbase.HFileWriter
import global.Configs
import org.apache.spark.sql.SparkSession
import Configs._

object CsvDAO extends WithConversionHelper with HFileWriter with  DataFrameHelper {

  def csvToHFile(implicit spark: SparkSession) {

    val louDF = spark.read.option("header", "true").csv(PATH_TO_LOU_CSV)
    val entDF = spark.read.option("header", "true").csv(PATH_TO_ENT_CSV)
    val reuDF = spark.read.option("header", "true").csv(PATH_TO_REU_CSV)
    val divisions = spark.read.option("header", "true").csv("src/main/resources/data/div.csv")
    val df = louDF.select("ern", "sic07", "employees")

    val sicRDD = getSection(df, divisions).coalesce(df.rdd.getNumPartitions)
    val entRDD = entDF.join(sicRDD, Seq("ern"), joinType="leftOuter").dropDuplicates("ern", "sic07")
      .rdd.map(row => toRecord(row, "ent")).cache
    val louRDD = spark.read.option("header", "true").csv(PATH_TO_LOU_CSV).rdd.map(row => toRecord(row, "lou")).cache
    val reuRDD = reuDF.rdd.map(row => toRecord(row, "reu")).cache

    toHFile(entRDD, PATH_TO_ENT_HFILE)
    toHFile(louRDD, PATH_TO_LOU_HFILE)
    toHFile(reuRDD, PATH_TO_REU_HFILE)
    toLinksHFile(entRDD, PATH_TO_LINKS_ENT_HFILE)
    toLinksHFile(louRDD, PATH_TO_LINKS_LOU_HFILE)
    toLinksHFile(reuRDD, PATH_TO_LINKS_REU_HFILE)

    entRDD.unpersist()
    louRDD.unpersist()
    reuRDD.unpersist()
  }
}
