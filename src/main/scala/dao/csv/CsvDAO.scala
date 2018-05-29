package dao.csv

import dao.hbase.converter.WithConversionHelper
import spark.calculations.DataFrameHelper
import dao.hbase.HFileWriter
import global.Configs
import org.apache.spark.sql.SparkSession
import Configs._
import org.apache.spark.sql.functions._

object CsvDAO extends WithConversionHelper with HFileWriter with  DataFrameHelper {

  def csvToHFile(implicit spark: SparkSession) {

    val louDF = spark.read.option("header", "true").csv(PATH_TO_LOU_CSV)
    val reuDF = spark.read.option("header", "true").csv(PATH_TO_REU_CSV)
    val entDF = spark.read.option("header", "true").csv(PATH_TO_ENT_CSV).withColumnRenamed("sic07", "temp_sic")
    val df = louDF.select("ern","sic07","employees")

    val sicRDD = getClassification(df).coalesce(df.rdd.getNumPartitions)
    val entRDD = entDF.join(sicRDD, Seq("ern"), joinType="leftOuter").dropDuplicates("ern","sic07").withColumn("sic", coalesce(col("sic07"), col("temp_sic")))
    val groupedLEU = groupLEU(entDF)
    val louRDD = louDF.rdd.map(row => toRecord(row, "lou")).cache
    val leuRDD = groupedLEU.join(entRDD,Seq("ern"),joinType = "outer").rdd.map(row => toRecord(row, "ent")).cache
    val reuRDD = reuDF.rdd.map(row => toRecord(row, "reu")).cache

    toHFile(leuRDD, PATH_TO_ENT_HFILE)
    toHFile(louRDD, PATH_TO_LOU_HFILE)
    toHFile(reuRDD, PATH_TO_REU_HFILE)

    toLinksHFile(leuRDD, PATH_TO_LINKS_ENT_HFILE)
    toLinksHFile(louRDD, PATH_TO_LINKS_LOU_HFILE)
    toLinksHFile(reuRDD, PATH_TO_LINKS_REU_HFILE)

    sicRDD.unpersist()
    entRDD.unpersist()
    leuRDD.unpersist()
    louRDD.unpersist()
    reuRDD.unpersist()
  }
}
