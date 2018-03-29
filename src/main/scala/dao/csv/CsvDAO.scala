package dao.csv

/**
  * Created by chohab on 20/03/2018.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import global.Configs
import dao.parquet.ParquetDAO

object CsvDAO {

  import Configs._
  val logger = LoggerFactory.getLogger(getClass)

  def csvToParquet(implicit spark:SparkSession) {
    val leuToEnt = spark.read.option("header","true").csv(PATH_TO_CSV)
    val groupedData = leuToEnt.groupBy("entref").agg(collect_list("ubrn").alias("ubrn")).withColumnRenamed("entref", "entref2")
    val leuToEntNoDuplicates = leuToEnt.drop("ubrn").dropDuplicates(Array("entref"))
    val outputData = leuToEntNoDuplicates.join(groupedData, col("entref") === col("entref2")).drop("entref2")
    outputData.write.mode("overwrite").parquet(PATH_TO_PARQUET)
    ParquetDAO.parquetToHFile(spark)
  }

}
