package dao.parquet

import dao.hbase.converter.WithConvertionHelper
import global.Configs
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, LongType, StringType,StructType, StructField, IntegerType}
import org.slf4j.LoggerFactory



object ParquetDAO extends WithConvertionHelper{

  import Configs._

  val logger = LoggerFactory.getLogger(getClass)

  def parquetToHFile(spark:SparkSession){

    val entRDD = spark.read.option("header","true").csv(PATH_TO_ENT_CSV).rdd.map(toEnterprise).cache
    val louRDD = spark.read.option("header","true").csv(PATH_TO_LOU_CSV).rdd.map(toLocalUnit).cache

    entRDD.flatMap(_.links).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
          .saveAsNewAPIHadoopFile(PATH_TO_LINKS_ENT_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    entRDD.flatMap(_.enterprises).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
          .saveAsNewAPIHadoopFile(PATH_TO_ENT_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    louRDD.flatMap(_.links).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(PATH_TO_LINKS_LOU_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    louRDD.flatMap(_.enterprises).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(PATH_TO_LOU_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    entRDD.unpersist()
    louRDD.unpersist()
  }
}
