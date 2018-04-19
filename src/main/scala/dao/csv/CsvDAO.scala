package dao.csv

import dao.hbase.converter.WithConversionHelper
import spark.calculations.DataFrameHelper
import dao.hbase.HFileWriter
import global.Configs
import org.apache.spark.sql.SparkSession
object CsvDAO extends WithConversionHelper with HFileWriter with  DataFrameHelper{

  import Configs._

  def csvToHFile(implicit spark: SparkSession) {

    val louDF = spark.read.option("header", "true").csv(PATH_TO_LOU_CSV)
    val entDF = spark.read.option("header", "true").csv(PATH_TO_ENT_CSV)
    val divisions = spark.read.option("header","true").csv("src/main/resources/data/div.csv")
    val df = louDF.select("ern","sic07","employees")

    val sicRDD = getSection(df, divisions)
    val entRDD = entDF.join(sicRDD, "ern").dropDuplicates("ern","sic07").coalesce(df.rdd.getNumPartitions).rdd.map(row => toRecord(row, "ent")).cache
    val louRDD = spark.read.option("header", "true").csv(PATH_TO_LOU_CSV).rdd.map(row => toRecord(row, "lou")).cache

    toHFile(entRDD, PATH_TO_ENT_HFILE)
    toHFile(louRDD, PATH_TO_LOU_HFILE)
    toLinksHFile(entRDD, PATH_TO_LINKS_ENT_HFILE)
    toLinksHFile(louRDD, PATH_TO_LINKS_LOU_HFILE)

    entRDD.unpersist()
    louRDD.unpersist()
  }
}
