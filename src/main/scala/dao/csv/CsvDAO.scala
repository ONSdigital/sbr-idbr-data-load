package dao.csv

import dao.hbase.converter.WithConversionHelper
import spark.calculations.DataFrameHelper
import dao.hbase.HFileWriter
import global.Configs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import Configs._

object CsvDAO extends WithConversionHelper with HFileWriter with  DataFrameHelper{

  def csvToHFile(implicit spark: SparkSession) {

    val louDF = spark.read.option("header", "true").csv(PATH_TO_LOU_CSV)
    val entDF = spark.read.option("header", "true").csv(PATH_TO_ENT_CSV)
    //.withColumnRenamed("sic07", "temp_sic")
    val divisions = spark.read.option("header","true").csv("src/main/resources/data/div.csv")
    val df = louDF.select("ern","sic07","employees")

    val sicRDD = getSection(df, divisions).coalesce(df.rdd.getNumPartitions)
    val entRDD = entDF.join(sicRDD, Seq("ern"), joinType="leftOuter").dropDuplicates("ern","sic07")
      //.withColumn("sic", coalesce(col("sic07"), col("temp_sic")))
    val groupedLEU = groupLEU(entDF)
    val louRDD = louDF.rdd.map(row => toRecord(row, "lou")).cache
    val leuRDD = groupedLEU.join(entRDD,Seq("ern"),joinType = "outer").rdd.map(row => toRecord(row, "ent")).cache

    toHFile(leuRDD, PATH_TO_ENT_HFILE)
    toHFile(louRDD, PATH_TO_LOU_HFILE)
    toLinksHFile(leuRDD, PATH_TO_LINKS_ENT_HFILE)
    toLinksHFile(louRDD, PATH_TO_LINKS_LOU_HFILE)

    entRDD.unpersist()
    louRDD.unpersist()
  }
}
