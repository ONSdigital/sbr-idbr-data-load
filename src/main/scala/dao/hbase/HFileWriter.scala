package dao.hbase

import org.apache.hadoop.hbase.KeyValue

import global.Configs
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD

trait HFileWriter {

  def toHFile(rdd: RDD[model.Tables], path: String): Unit = {
    rdd.flatMap(_.units).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(path,classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)
  }

  def toHFileSingle(rdd: RDD[model.TableSingle], path: String): Unit = {
    rdd.flatMap(_.units).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(path,classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)
  }

  def toLinksHFile(rdd: RDD[model.Tables], path: String): Unit = {
    rdd.flatMap(_.links).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(path, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)
  }
}
