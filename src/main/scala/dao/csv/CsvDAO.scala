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
    val entFileDF = spark.read.option("header", "true").csv(PATH_TO_ENT_CSV).withColumnRenamed("sic07", "temp_sic")
    val df = louDF.select("ern","sic07","employees")

    val sicRDD = getClassification(df).coalesce(df.rdd.getNumPartitions)
    val leuRDDFile  = entFileDF.join(sicRDD, Seq("ern"), joinType="leftOuter").withColumn("sic", coalesce(col("sic07"), col("temp_sic")))
    val entSICRDD = entFileDF.join(sicRDD, Seq("ern"), joinType="leftOuter").dropDuplicates("ern","sic07").withColumn("sic", coalesce(col("sic07"), col("temp_sic")))
    val groupedLEU = groupLEU(entFileDF)
    val ruForLOUDF = reuDF.select("ern","rurn","ruref")
    val louRUDF = louDF.join(ruForLOUDF, Seq("ern"), joinType = "leftOuter").dropDuplicates("lou")
    val louRDD = louRUDF.rdd.map(row => toRecord(row, "lou")).cache
    val entRDD = groupedLEU.join(entSICRDD,Seq("ern"),joinType = "outer").rdd.map(row => toRecord(row, "ent")).cache
    val reuRDD = reuDF.rdd.map(row => toRecord(row, "reu")).cache
    val leuRDD = leuRDDFile.rdd.map(row => toRecordSingle(row, "leu")).cache

    toHFileSingle(leuRDD, PATH_TO_LEU_HFILE)
    toHFile(entRDD, PATH_TO_ENT_HFILE)
    toHFile(louRDD, PATH_TO_LOU_HFILE)
    toHFile(reuRDD, PATH_TO_REU_HFILE)


    toLinksHFile(entRDD, PATH_TO_LINKS_ENT_HFILE)
    toLinksHFile(louRDD, PATH_TO_LINKS_LOU_HFILE)
    toLinksHFile(reuRDD, PATH_TO_LINKS_REU_HFILE)

    sicRDD.unpersist()
    entSICRDD.unpersist()
    entRDD.unpersist()
    louRDD.unpersist()
    reuRDD.unpersist()
  }
}
